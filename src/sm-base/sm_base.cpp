/*
   Tencent is pleased to support the open source community by making 
   PhxPaxos available.
   Copyright (C) 2016 THL A29 Limited, a Tencent company. 
   All rights reserved.

   Licensed under the BSD 3-Clause License (the "License"); you may 
   not use this file except in compliance with the License. You may 
   obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "commdef.h"
#include "sm_base.h"
#include <string.h>
#include "comm_include.h"

using namespace std;

namespace phxpaxos
{

SMFac :: SMFac(const int iMyGroupIdx) : m_iMyGroupIdx(iMyGroupIdx)
{
}

SMFac :: ~SMFac()
{
}

bool SMFac :: Execute(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sPaxosValue, SMCtx * poSMCtx)
{
  if (sPaxosValue.size() < sizeof(int))
  {
    PLG1Err("Value wrong, instanceid %lu size %zu", llInstanceID, sPaxosValue.size());
    //need do nothing, just skip
    return true;
  }

  // 首先从传入的value中把数据decode出来
  // 最开始的int时SMID
  int iSMID = 0;
  memcpy(&iSMID, sPaxosValue.data(), sizeof(int));

  if (iSMID == 0)
  {
    PLG1Imp("Value no need to do sm, just skip, instanceid %lu", llInstanceID);
    return true;
  }

  // 紧跟着的是body数据
  std::string sBodyValue = string(sPaxosValue.data() + sizeof(int), sPaxosValue.size() - sizeof(int));
  if (iSMID == BATCH_PROPOSE_SMID)
  {
    // 如果是batch提交
    BatchSMCtx * poBatchSMCtx = nullptr;
    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
    {
      poBatchSMCtx = (BatchSMCtx *)poSMCtx->m_pCtx;
    }
    return BatchExecute(iGroupIdx, llInstanceID, sBodyValue, poBatchSMCtx);
  }
  else
  {
    return DoExecute(iGroupIdx, llInstanceID, sBodyValue, iSMID, poSMCtx);
  }
}

// 批量提交
bool SMFac :: BatchExecute(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sBodyValue, BatchSMCtx * poBatchSMCtx)
{
  // 先decode出batch value
  BatchPaxosValues oBatchValues;
  bool bSucc = oBatchValues.ParseFromArray(sBodyValue.data(), sBodyValue.size());
  if (!bSucc)
  {
    PLG1Err("ParseFromArray fail, valuesize %zu", sBodyValue.size());
    return false;
  }

  if (poBatchSMCtx != nullptr) 
  {
    // 要判断一下批量提交的数据数量和前面decode出来的value数量是否一致
    if ((int)poBatchSMCtx->m_vecSMCtxList.size() != oBatchValues.values_size())
    {
      PLG1Err("values size %d not equal to smctx size %zu",
        oBatchValues.values_size(), poBatchSMCtx->m_vecSMCtxList.size());
      return false;
    }
  }

  // 到了这里，一个循环遍历里面的value逐个进行提交
  for (int i = 0; i < oBatchValues.values_size(); i++)
  {
    const PaxosValue & oValue = oBatchValues.values(i);
    SMCtx * poSMCtx = poBatchSMCtx != nullptr ? poBatchSMCtx->m_vecSMCtxList[i] : nullptr;
    bool bExecuteSucc = DoExecute(iGroupIdx, llInstanceID, oValue.value(), oValue.smid(), poSMCtx);
    if (!bExecuteSucc)
    {
      return false;
    }
  }

  return true;
}

// 提交数据的主函数
bool SMFac :: DoExecute(const int iGroupIdx, const uint64_t llInstanceID, 
  const std::string & sBodyValue, const int iSMID, SMCtx * poSMCtx)
{
  if (iSMID == 0)
  {
    PLG1Imp("Value no need to do sm, just skip, instanceid %lu", llInstanceID);
    return true;
  }

  if (m_vecSMList.size() == 0)
  {
    PLG1Imp("No any sm, need wait sm, instanceid %lu", llInstanceID);
    return false;
  }

  for (auto & poSM : m_vecSMList)
  {
    // 只有针对相同SMID的状态机才进行提交
    if (poSM->SMID() == iSMID)
    {
      return poSM->Execute(iGroupIdx, llInstanceID, sBodyValue, poSMCtx);
    }
  }

  PLG1Err("Unknown smid %d instanceid %lu", iSMID, llInstanceID);
  return false;
}


////////////////////////////////////////////////////////////////

bool SMFac :: ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sPaxosValue)
{
  if (sPaxosValue.size() < sizeof(int))
  {
    PLG1Err("Value wrong, instanceid %lu size %zu", llInstanceID, sPaxosValue.size());
    //need do nothing, just skip
    return true;
  }

  int iSMID = 0;
  memcpy(&iSMID, sPaxosValue.data(), sizeof(int));

  if (iSMID == 0)
  {
    PLG1Imp("Value no need to do sm, just skip, instanceid %lu", llInstanceID);
    return true;
  }

  std::string sBodyValue = string(sPaxosValue.data() + sizeof(int), sPaxosValue.size() - sizeof(int));
  if (iSMID == BATCH_PROPOSE_SMID)
  {
    return BatchExecuteForCheckpoint(iGroupIdx, llInstanceID, sBodyValue);
  }
  else
  {
    return DoExecuteForCheckpoint(iGroupIdx, llInstanceID, sBodyValue, iSMID);
  }
}

bool SMFac :: BatchExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
  const std::string & sBodyValue)
{
  BatchPaxosValues oBatchValues;
  bool bSucc = oBatchValues.ParseFromArray(sBodyValue.data(), sBodyValue.size());
  if (!bSucc)
  {
    PLG1Err("ParseFromArray fail, valuesize %zu", sBodyValue.size());
    return false;
  }

  for (int i = 0; i < oBatchValues.values_size(); i++)
  {
    const PaxosValue & oValue = oBatchValues.values(i);
    bool bExecuteSucc = DoExecuteForCheckpoint(iGroupIdx, llInstanceID, oValue.value(), oValue.smid());
    if (!bExecuteSucc)
    {
      return false;
    }
  }

  return true;
}

bool SMFac :: DoExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
  const std::string & sBodyValue, const int iSMID)
{
  if (iSMID == 0)
  {
    PLG1Imp("Value no need to do sm, just skip, instanceid %lu", llInstanceID);
    return true;
  }

  if (m_vecSMList.size() == 0)
  {
    PLG1Imp("No any sm, need wait sm, instanceid %lu", llInstanceID);
    return false;
  }

  for (auto & poSM : m_vecSMList)
  {
    if (poSM->SMID() == iSMID)
    {
      return poSM->ExecuteForCheckpoint(iGroupIdx, llInstanceID, sBodyValue);
    }
  }

  PLG1Err("Unknown smid %d instanceid %lu", iSMID, llInstanceID);

  return false;
}

////////////////////////////////////////////////////////

void SMFac :: PackPaxosValue(std::string & sPaxosValue, const int iSMID)
{
  char sSMID[sizeof(int)] = {0};
  if (iSMID != 0)
  {
    memcpy(sSMID, &iSMID, sizeof(sSMID));
  }

  sPaxosValue = string(sSMID, sizeof(sSMID)) + sPaxosValue;
}

void SMFac :: AddSM(StateMachine * poSM)
{
  for (auto & poSMt : m_vecSMList)
  {
    // 不能有多个相同SMID的状态机
    if (poSMt->SMID() == poSM->SMID())
    {
      return;
    }
  }

  m_vecSMList.push_back(poSM);
}

///////////////////////////////////////////////////////

const uint64_t SMFac :: GetCheckpointInstanceID(const int iGroupIdx) const
{
  uint64_t llCPInstanceID = -1;
  uint64_t llCPInstanceID_Insize = -1;
  bool bHaveUseSM = false;

  for (auto & poSM : m_vecSMList)
  {
    uint64_t llCheckpointInstanceID = poSM->GetCheckpointInstanceID(iGroupIdx);
    if (poSM->SMID() == SYSTEM_V_SMID
      || poSM->SMID() == MASTER_V_SMID)
    {
      //system variables 
      //master variables
      //if no user state machine, system and master's can use.
      //if have user state machine, use user'state machine's checkpointinstanceid.
      if (llCheckpointInstanceID == uint64_t(-1))
      {
        continue;
      }

      if (llCheckpointInstanceID > llCPInstanceID_Insize
        || llCPInstanceID_Insize == (uint64_t)-1)
      {
        llCPInstanceID_Insize = llCheckpointInstanceID;
      }

      continue;
    }

    bHaveUseSM = true;

    if (llCheckpointInstanceID == uint64_t(-1))
    {
      continue;
    }

    // 只返回SM中最大的实例ID
    if (llCheckpointInstanceID > llCPInstanceID
      || llCPInstanceID == (uint64_t)-1)
    {
      llCPInstanceID = llCheckpointInstanceID;
    }
  }

  return bHaveUseSM ? llCPInstanceID : llCPInstanceID_Insize;
}

std::vector<StateMachine *> SMFac :: GetSMList()
{
  return m_vecSMList;
}

}


