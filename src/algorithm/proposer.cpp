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

#include "proposer.h"
#include "learner.h"
#include "phxpaxos/sm.h"
#include "instance.h"

namespace phxpaxos
{

ProposerState :: ProposerState(const Config * poConfig)
{
    m_poConfig = (Config *)poConfig;
    m_llProposalID = 1;
    Init();
}

ProposerState :: ~ProposerState()
{
}

void ProposerState :: Init()
{
    m_llHighestOtherProposalID = 0;
    m_sValue.clear();
}

void ProposerState ::  SetStartProposalID(const uint64_t llProposalID)
{
    m_llProposalID = llProposalID;
}

void ProposerState :: NewPrepare()
{
    PLGHead("START ProposalID %lu HighestOther %lu MyNodeID %lu",
            m_llProposalID, m_llHighestOtherProposalID, m_poConfig->GetMyNodeID());
        
    uint64_t llMaxProposalID =
        m_llProposalID > m_llHighestOtherProposalID ? m_llProposalID : m_llHighestOtherProposalID;

    // 新的propose ID为前面最大提交ID+1
    m_llProposalID = llMaxProposalID + 1;

    PLGHead("END New.ProposalID %lu", m_llProposalID);

}

void ProposerState :: AddPreAcceptValue(
        const BallotNumber & oOtherPreAcceptBallot, 
        const std::string & sOtherPreAcceptValue)
{
    PLGDebug("OtherPreAcceptID %lu OtherPreAcceptNodeID %lu HighestOtherPreAcceptID %lu "
            "HighestOtherPreAcceptNodeID %lu OtherPreAcceptValue %zu",
            oOtherPreAcceptBallot.m_llProposalID, oOtherPreAcceptBallot.m_llNodeID,
            m_oHighestOtherPreAcceptBallot.m_llProposalID, m_oHighestOtherPreAcceptBallot.m_llNodeID, 
            sOtherPreAcceptValue.size());

    if (oOtherPreAcceptBallot.isnull())
    {
        return;
    }
    
    // 要大于当前保存的才能保存下来
    if (oOtherPreAcceptBallot > m_oHighestOtherPreAcceptBallot)
    {
        m_oHighestOtherPreAcceptBallot = oOtherPreAcceptBallot;
        m_sValue = sOtherPreAcceptValue;
    }
}

const uint64_t ProposerState :: GetProposalID()
{
    return m_llProposalID;
}

const std::string & ProposerState :: GetValue()
{
    return m_sValue;
}

void ProposerState :: SetValue(const std::string & sValue)
{
    m_sValue = sValue;
}

void ProposerState :: SetOtherProposalID(const uint64_t llOtherProposalID)
{
    // 只有比原来的大才能保存
    if (llOtherProposalID > m_llHighestOtherProposalID)
    {
        m_llHighestOtherProposalID = llOtherProposalID;
    }
}

void ProposerState :: ResetHighestOtherPreAcceptBallot()
{
    m_oHighestOtherPreAcceptBallot.reset();
}

////////////////////////////////////////////////////////////////

Proposer :: Proposer(
        const Config * poConfig, 
        const MsgTransport * poMsgTransport,
        const Instance * poInstance,
        const Learner * poLearner,
        const IOLoop * poIOLoop)
    : Base(poConfig, poMsgTransport, poInstance), m_oProposerState(poConfig), m_oMsgCounter(poConfig)
{
    m_poLearner = (Learner *)poLearner;
    m_poIOLoop = (IOLoop *)poIOLoop;
    
    m_bIsPreparing = false;
    m_bIsAccepting = false;

    m_bCanSkipPrepare = false;

    InitForNewPaxosInstance();

    m_iPrepareTimerID = 0;
    m_iAcceptTimerID = 0;
    m_llTimeoutInstanceID = 0;

    m_iLastPrepareTimeoutMs = m_poConfig->GetPrepareTimeoutMs();
    m_iLastAcceptTimeoutMs = m_poConfig->GetAcceptTimeoutMs();

    m_bWasRejectBySomeone = false;
}

Proposer :: ~Proposer()
{
}

void Proposer :: SetStartProposalID(const uint64_t llProposalID)
{
    m_oProposerState.SetStartProposalID(llProposalID);
}

// 初始化实例的状态
void Proposer :: InitForNewPaxosInstance()
{
    m_oMsgCounter.StartNewRound();
    m_oProposerState.Init();

    ExitPrepare();
    ExitAccept();
}

// 在准备阶段或者提交阶段
bool Proposer :: IsWorking()
{
    return m_bIsPreparing || m_bIsAccepting;
}

// 提交一个新的值
int Proposer :: NewValue(const std::string & sValue)
{
    BP->GetProposerBP()->NewProposal(sValue);

    if (m_oProposerState.GetValue().size() == 0)
    {
        m_oProposerState.SetValue(sValue);
    }

    // 超时时间
    m_iLastPrepareTimeoutMs = START_PREPARE_TIMEOUTMS;
    m_iLastAcceptTimeoutMs = START_ACCEPT_TIMEOUTMS;

    if (m_bCanSkipPrepare && !m_bWasRejectBySomeone)
    {
        // 在可以忽略而且没有被任何一个acceptor拒绝的情况下才能直接接受
        BP->GetProposerBP()->NewProposalSkipPrepare();

        PLGHead("skip prepare, directly start accept");
        Accept();
    }
    else
    {
        //if not reject by someone, no need to increase ballot
        Prepare(m_bWasRejectBySomeone);
    }

    return 0;
}

void Proposer :: ExitPrepare()
{
    if (m_bIsPreparing)
    {
        m_bIsPreparing = false;
        
        // 移除准备阶段设置的timer
        m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
    }
}

void Proposer :: ExitAccept()
{
    if (m_bIsAccepting)
    {
        m_bIsAccepting = false;
        
        // 移除接受阶段设置的timer
        m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
    }
}

void Proposer :: AddPrepareTimer(const int iTimeoutMs)
{
    if (m_iPrepareTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
    }

    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Prepare_Timeout,
                m_iPrepareTimerID);
        return;
    }

    m_poIOLoop->AddTimer(
            m_iLastPrepareTimeoutMs,
            Timer_Proposer_Prepare_Timeout,
            m_iPrepareTimerID);

    m_llTimeoutInstanceID = GetInstanceID();

    PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

    // 每一次将超时时间翻倍
    m_iLastPrepareTimeoutMs *= 2;
    if (m_iLastPrepareTimeoutMs > MAX_PREPARE_TIMEOUTMS)
    {
        // 但是不能超过阈值
        m_iLastPrepareTimeoutMs = MAX_PREPARE_TIMEOUTMS;
    }
}

void Proposer :: AddAcceptTimer(const int iTimeoutMs)
{
    if (m_iAcceptTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
    }

    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Accept_Timeout,
                m_iAcceptTimerID);
        return;
    }

    m_poIOLoop->AddTimer(
            m_iLastAcceptTimeoutMs,
            Timer_Proposer_Accept_Timeout,
            m_iAcceptTimerID);

    m_llTimeoutInstanceID = GetInstanceID();
    
    PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

    m_iLastAcceptTimeoutMs *= 2;
    if (m_iLastAcceptTimeoutMs > MAX_ACCEPT_TIMEOUTMS)
    {
        m_iLastAcceptTimeoutMs = MAX_ACCEPT_TIMEOUTMS;
    }
}

void Proposer :: Prepare(const bool bNeedNewBallot)
{
    PLGHead("START Now.InstanceID %lu MyNodeID %lu State.ProposalID %lu State.ValueLen %zu",
            GetInstanceID(), m_poConfig->GetMyNodeID(), m_oProposerState.GetProposalID(),
            m_oProposerState.GetValue().size());

    BP->GetProposerBP()->Prepare();
    m_oTimeStat.Point();
    
    // 先清空一些状态
    ExitAccept();
    m_bIsPreparing = true;
    m_bCanSkipPrepare = false;
    m_bWasRejectBySomeone = false;

    m_oProposerState.ResetHighestOtherPreAcceptBallot();
    if (bNeedNewBallot)
    {
        m_oProposerState.NewPrepare();
    }

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosPrepare);
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());

    m_oMsgCounter.StartNewRound();

    // 添加准备阶段的定时器
    AddPrepareTimer();

    PLGHead("END OK");

    // 将prepare消息广播出去
    BroadcastMessage(oPaxosMsg);
}

// 收到prepare消息时的处理
void Proposer :: OnPrepareReply(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
            oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(), 
            oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

    BP->GetProposerBP()->OnPrepareReply();
    
    if (!m_bIsPreparing)
   {
      // 没有在准备阶段
        BP->GetProposerBP()->OnPrepareReplyButNotPreparing();
        //PLGErr("Not preparing, skip this msg");
        return;
    }

    if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID())
    {
      // 提交的ID不对
        BP->GetProposerBP()->OnPrepareReplyNotSameProposalIDMsg();
        //PLGErr("ProposalID not same, skip this msg");
        return;
    }

    // 保存到计数器
    m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        // 没有被拒绝，保存下来ID和值
        BallotNumber oBallot(oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid());
        PLGDebug("[Promise] PreAcceptedID %lu PreAcceptedNodeID %lu ValueSize %zu", 
                oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid(), oPaxosMsg.value().size());
        m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
        m_oProposerState.AddPreAcceptValue(oBallot, oPaxosMsg.value());
    }
    else
    {
        // 拒绝了
        PLGDebug("[Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
        m_oMsgCounter.AddReject(oPaxosMsg.nodeid());
        m_bWasRejectBySomeone = true;
        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }

    if (m_oMsgCounter.IsPassedOnThisRound())
    {
        // 根据计数器查询这一轮通过
        int iUseTimeMs = m_oTimeStat.Point();
        BP->GetProposerBP()->PreparePass(iUseTimeMs);
        PLGImp("[Pass] start accept, usetime %dms", iUseTimeMs);
        m_bCanSkipPrepare = true;
        Accept();
    }
    else if (m_oMsgCounter.IsRejectedOnThisRound()
            || m_oMsgCounter.IsAllReceiveOnThisRound())
    {
        // 添加准备阶段的定时器
        BP->GetProposerBP()->PrepareNotPass();
        PLGImp("[Not Pass] wait 30ms and restart prepare");
        AddPrepareTimer(OtherUtils::FastRand() % 30 + 10);
    }

    PLGHead("END");
}

void Proposer :: Accept()
{
    PLGHead("START ProposalID %lu ValueSize %zu ValueLen %zu", 
            m_oProposerState.GetProposalID(), m_oProposerState.GetValue().size(), m_oProposerState.GetValue().size());

    BP->GetProposerBP()->Accept();
    m_oTimeStat.Point();
    
    // 退出准备状态
    ExitPrepare();
    m_bIsAccepting = true;
    
    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosAccept);
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());
    oPaxosMsg.set_value(m_oProposerState.GetValue());
    oPaxosMsg.set_lastchecksum(GetLastChecksum());

    // 清空计数器，开始新的一轮
    m_oMsgCounter.StartNewRound();

    // 添加接受定时器
    AddAcceptTimer();

    PLGHead("END");

    // 广播这个消息
    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_Final);
}

// 接收到accept消息时的处理
void Proposer :: OnAcceptReply(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
            oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(), 
            oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

    BP->GetProposerBP()->OnAcceptReply();

    if (!m_bIsAccepting)
    {
        // 没有在接受状态
        //PLGErr("Not proposing, skip this msg");
        BP->GetProposerBP()->OnAcceptReplyButNotAccepting();
        return;
    }

    if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID())
    {
        // ID不对
        //PLGErr("ProposalID not same, skip this msg");
        BP->GetProposerBP()->OnAcceptReplyNotSameProposalIDMsg();
        return;
    }

    // 计数器接收这个消息
    m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        // 没有被拒绝
        PLGDebug("[Accept]");
        m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
    }
    else
    {
        PLGDebug("[Reject]");
        m_oMsgCounter.AddReject(oPaxosMsg.nodeid());

        m_bWasRejectBySomeone = true;

        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }

    if (m_oMsgCounter.IsPassedOnThisRound())
    {
        // 提议通过了
        int iUseTimeMs = m_oTimeStat.Point();
        BP->GetProposerBP()->AcceptPass(iUseTimeMs);
        PLGImp("[Pass] Start send learn, usetime %dms", iUseTimeMs);
        ExitAccept();
        // 通过learn学习新提交成功的值
        m_poLearner->ProposerSendSuccess(GetInstanceID(), m_oProposerState.GetProposalID());
    }
    else if (m_oMsgCounter.IsRejectedOnThisRound()
            || m_oMsgCounter.IsAllReceiveOnThisRound())
    {
        BP->GetProposerBP()->AcceptNotPass();
        PLGImp("[Not pass] wait 30ms and Restart prepare");
        AddAcceptTimer(OtherUtils::FastRand() % 30 + 10);
    }

    PLGHead("END");
}

// prepare超时
void Proposer :: OnPrepareTimeout()
{
    PLGHead("OK");

    if (GetInstanceID() != m_llTimeoutInstanceID)
    {
        PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
                m_llTimeoutInstanceID, GetInstanceID());
        return;
    }

    BP->GetProposerBP()->PrepareTimeout();
    
    // 重新发起prepare
    Prepare(m_bWasRejectBySomeone);
}

// accept超时
void Proposer :: OnAcceptTimeout()
{
    PLGHead("OK");
    
    if (GetInstanceID() != m_llTimeoutInstanceID)
    {
        PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
                m_llTimeoutInstanceID, GetInstanceID());
        return;
    }
    
    BP->GetProposerBP()->AcceptTimeout();
    
    // 重新发起prepare
    Prepare(m_bWasRejectBySomeone);
}

void Proposer :: CancelSkipPrepare()
{
    m_bCanSkipPrepare = false;
}

}


