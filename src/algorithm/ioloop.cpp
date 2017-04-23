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

#include "ioloop.h"
#include "utils_include.h"
#include "instance.h"

using namespace std;

namespace phxpaxos
{

IOLoop :: IOLoop(Config * poConfig, Instance * poInstance)
    : m_poConfig(poConfig), m_poInstance(poInstance)
{
    m_bIsEnd = false;
    m_bIsStart = false;

    m_iQueueMemSize = 0;
}

IOLoop :: ~IOLoop()
{
}

// IO主函数
void IOLoop :: run()
{
    m_bIsEnd = false;
    m_bIsStart = true;
    while(true)
    {
        BP->GetIOLoopBP()->OneLoop();

        int iNextTimeout = 1000;
        
        // 处理超时消息
        DealwithTimeout(iNextTimeout);

        //PLGHead("nexttimeout %d", iNextTimeout);

        // 处理下一次循环
        OneLoop(iNextTimeout);

        if (m_bIsEnd)
        {
            PLGHead("IOLoop [End]");
            break;
        }
    }
}

void IOLoop :: AddNotify()
{
    m_oMessageQueue.lock();
    // 添加一个null消息，以唤醒队列？？
    m_oMessageQueue.add(nullptr);
    m_oMessageQueue.unlock();
}

int IOLoop :: AddMessage(const char * pcMessage, const int iMessageLen)
{
    m_oMessageQueue.lock();

    BP->GetIOLoopBP()->EnqueueMsg();

    // 不要超过队列容量
    if ((int)m_oMessageQueue.size() > QUEUE_MAXLENGTH)
    {
        BP->GetIOLoopBP()->EnqueueMsgRejectByFullQueue();

        PLGErr("Queue full, skip msg");
        m_oMessageQueue.unlock();
        return -2;
    }

    // 不要超过队列内存容量
    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE)
    {
        PLErr("queue memsize %d too large, can't enqueue", m_iQueueMemSize);
        m_oMessageQueue.unlock();
        return -2;
    }
    
    m_oMessageQueue.add(new string(pcMessage, iMessageLen));

    m_iQueueMemSize += iMessageLen;

    m_oMessageQueue.unlock();

    return 0;
}

// 添加需要重试的paxos消息
int IOLoop :: AddRetryPaxosMsg(const PaxosMsg & oPaxosMsg)
{
    BP->GetIOLoopBP()->EnqueueRetryMsg();

    // 如果超出大小，那么先pop一个出来
    if (m_oRetryQueue.size() > RETRY_QUEUE_MAX_LEN)
    {
        BP->GetIOLoopBP()->EnqueueRetryMsgRejectByFullQueue();
        m_oRetryQueue.pop();
    }
    
    m_oRetryQueue.push(oPaxosMsg);
    return 0;
}

void IOLoop :: Stop()
{
    m_bIsEnd = true;
    if (m_bIsStart)
    {
        join();
    }
}

void IOLoop :: ClearRetryQueue()
{
    while (!m_oRetryQueue.empty())
    {
        m_oRetryQueue.pop();
    }
}

// 处理retry消息
void IOLoop :: DealWithRetry()
{
    if (m_oRetryQueue.empty())
    {
        return;
    }
    
    bool bHaveRetryOne = false;
    // 从retry队列中取出消息进行处理
    while (!m_oRetryQueue.empty())
    {
        PaxosMsg & oPaxosMsg = m_oRetryQueue.front();
        // 消息ID大于当前实例ID+1,退出循环
        if (oPaxosMsg.instanceid() > m_poInstance->GetNowInstanceID() + 1)
        {
            break;
        }
        // 消息ID等于当前实例ID+1
        else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID() + 1)
        {
            //only after retry i == now_i, than we can retry i + 1.
            if (bHaveRetryOne)
            {
                // 只有在之前已经有处理retry消息的情况下才进行处理
                BP->GetIOLoopBP()->DealWithRetryMsg();
                PLGDebug("retry msg (i+1). instanceid %lu", oPaxosMsg.instanceid());
                m_poInstance->OnReceivePaxosMsg(oPaxosMsg, true);
            }
            else
            {
                // 否则退出循环
                break;
            }
        }
        // 消息ID等于当前实例ID
        else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID())
        {
            // 处理这个消息
            BP->GetIOLoopBP()->DealWithRetryMsg();
            PLGDebug("retry msg. instanceid %lu", oPaxosMsg.instanceid());
            m_poInstance->OnReceivePaxosMsg(oPaxosMsg);
            bHaveRetryOne = true;
        }

        m_oRetryQueue.pop();
    }
}

void IOLoop :: OneLoop(const int iTimeoutMs)
{
    std::string * psMessage = nullptr;

    m_oMessageQueue.lock();
    // 在消息队列中最长等待等待iTimeoutMs时间
    bool bSucc = m_oMessageQueue.peek(psMessage, iTimeoutMs);
    
    if (!bSucc)
    {
        m_oMessageQueue.unlock();
    }
    else
    {
        m_oMessageQueue.pop();
        m_oMessageQueue.unlock();

        if (psMessage != nullptr && psMessage->size() > 0)
        {
            m_iQueueMemSize -= psMessage->size();
            // 处理消息
            m_poInstance->OnReceive(*psMessage);
        }

        delete psMessage;

        BP->GetIOLoopBP()->OutQueueMsg();
    }

    DealWithRetry();

    //must put on here
    //because addtimer on this funciton
    m_poInstance->CheckNewValue();
}

bool IOLoop :: AddTimer(const int iTimeout, const int iType, uint32_t & iTimerID)
{
    if (iTimeout == -1)
    {
        return true;
    }
    
    uint64_t llAbsTime = Time::GetSteadyClockMS() + iTimeout;
    m_oTimer.AddTimerWithType(llAbsTime, iType, iTimerID);

    m_mapTimerIDExist[iTimerID] = true;

    return true;
}

void IOLoop :: RemoveTimer(uint32_t & iTimerID)
{
    auto it = m_mapTimerIDExist.find(iTimerID);
    if (it != end(m_mapTimerIDExist))
    {
        m_mapTimerIDExist.erase(it);
    }

    iTimerID = 0;
}

void IOLoop :: DealwithTimeoutOne(const uint32_t iTimerID, const int iType)
{
    // 根据ID查找超时信息
    auto it = m_mapTimerIDExist.find(iTimerID);
    if (it == end(m_mapTimerIDExist))
    {
        //PLGErr("Timeout aready remove!, timerid %u iType %d", iTimerID, iType);
        return;
    }

    // 删除超时消息
    m_mapTimerIDExist.erase(it);

    m_poInstance->OnTimeout(iTimerID, iType);
}

// 处理超时
void IOLoop :: DealwithTimeout(int & iNextTimeout)
{
    bool bHasTimeout = true;

    while(bHasTimeout)
    {
        uint32_t iTimerID = 0;
        int iType = 0;
        // 取出一个超时消息ID
        bHasTimeout = m_oTimer.PopTimeout(iTimerID, iType);

        if (bHasTimeout)
        {
            // 处理这个超时消息
            DealwithTimeoutOne(iTimerID, iType);

            // 拿到下一个超时信息
            iNextTimeout = m_oTimer.GetNextTimeout();
            if (iNextTimeout != 0)
            {
                // 没有就退出循环
                break;
            }
        }
    }
}

}


