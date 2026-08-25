#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
【零基础保姆级教程代码】Mini Coroutine Scheduler（微型用户态协程调度器）
所属学科：《操作系统》—— 进程与线程管理、状态机流转、上下文切换与事件循环

💡【给零基础同学的前言】：
  为什么现代后端框架（如 FastAPI、Node.js、Go 语言的 Goroutine）并发能力那么强？
  传统的“多进程/多线程”每创建一个就要向操作系统申请 8MB 栈内存，且内核级上下文切换极其昂贵；
  而“协程（Coroutine）”完全是在【用户态（应用程序自己）】管理的轻量级任务，内存占用只有几百字节！
  本文件【纯手工从零手写调度内核】，带你一步步看懂：
  1. 什么是任务控制块（TCB / task_struct）？
  2. 操作系统课本上的【5 大进程状态（就绪、运行、睡眠、阻塞、终止）】是如何在代码里流转的？
  3. 什么是上下文切换（Context Switch）？—— Python 的 `yield` 是如何实现“任务主动交出 CPU，下次再从原地继续执行”的？
  4. 什么是事件循环（Event Loop）？—— 为什么需要用“最小堆（Min-Heap）”来做定时器唤醒？
================================================================================
"""

import time
import heapq
from enum import Enum, auto
from collections import deque
from typing import Any, Callable, Generator, List, Optional, Tuple


# ==============================================================================
# 🌟 第一部分：任务生命周期状态机 (Task State Machine)
# ==============================================================================
# 【操作系统核心理论】：
#   一个进程或线程在操作系统内部绝不是一成不变的，它具有严格的状态生命周期：
#   ------------------------------------------------------------------------
#   [创建] -> READY (就绪态)  <-- 资源已就绪，在就绪队列中排队等待 CPU 时间片
#                 |
#                 v (调度器选中执行)
#             RUNNING (运行态) <-- 正在占用 CPU 执行代码
#                 |
#                 +---> SLEEPING (睡眠态)  <-- 调用 sleep(2)，挂入时间堆等待定时唤醒
#                 |
#                 +---> WAITING (阻塞/等待态) <-- 等待网络数据、锁或队列数据 (I/O Wait)
#                 |
#                 v (代码执行完毕)
#             TERMINATED (终止态) <-- 任务结束，释放资源
#   ------------------------------------------------------------------------
# ==============================================================================
class TaskState(Enum):
    READY = auto()        # 就绪态：万事俱备，只欠 CPU 时间片
    RUNNING = auto()      # 运行态：正在执行当前协程代码
    SLEEPING = auto()     # 睡眠态：进入了定时器等待队列
    WAITING = auto()      # 阻塞等待态：在等待锁、事件或队列里的数据
    TERMINATED = auto()   # 终止态：任务已正常结束或报错退出


# ==============================================================================
# 🌟 第二部分：任务控制块 (TCB - Task Control Block)
# ==============================================================================
# 【操作系统理论】：
#   在 Linux 内核中，每个进程都对应一个 `struct task_struct` 结构体，用来记录它的 PID、状态、寄存器。
#   在我们的微型调度器中，Task 类就是我们自己的 TCB！
# ==============================================================================
class Task:
    _id_counter = 1 # 全局唯一任务 ID (TID) 自增生成器

    def __init__(self, coro: Generator, name: Optional[str] = None):
        self.tid = Task._id_counter
        Task._id_counter += 1
        self.name = name or f"Task-{self.tid}"      # 任务名称（如 "Producer-1"）
        self.coro = coro                            # 任务的协程生成器对象（底层保存了局部变量和执行指针 PC）
        self.state = TaskState.READY                # 任务初始状态为就绪态 (READY)
        self.wake_time: Optional[float] = None      # 如果进入了睡眠态，记录应该被唤醒的绝对时间戳
        self.result: Any = None                     # 任务正常结束时的返回值
        self.exception: Optional[Exception] = None  # 如果任务崩溃，记录异常信息
        self.steps_executed = 0                     # 累计运行的时间片次数（执行步数）

    def resume(self, send_val: Any = None) -> Any:
        """
        【上下文切换切入 (Context Switch In)】：
        唤醒该任务，让它占用 CPU 继续向下执行，直到它遇到下一个 yield 主动交出控制权。
        """
        self.state = TaskState.RUNNING
        self.steps_executed += 1
        return self.coro.send(send_val)

    def __repr__(self):
        return f"<Task {self.name}(TID={self.tid}) State={self.state.name}>"


# ==============================================================================
# 🌟 第三部分：微型用户态调度器与事件循环 (Scheduler & Event Loop)
# ==============================================================================
class Scheduler:
    def __init__(self):
        # 1. 【就绪队列 (Ready Queue)】：使用双端队列 deque，实现 FIFO (先来先服务) 轮转调度
        self.ready_queue: deque[Task] = deque()

        # 2. 【睡眠定时器最小堆 (Sleeping Min-Heap)】：
        # 💡【算法考点：为什么定时器必须用最小堆（Priority Queue / Heap）？】
        # 如果有 10000 个任务在 sleep，如果用普通列表，每毫秒都要遍历 10000 次（O(N) 性能崩溃！）。
        # 用最小堆，堆顶永远是“最早醒来的那一个任务”！
        # 调度器每次只需要看堆顶 `heap[0]` 到时间没有（O(1) 检查，O(log N) 弹出），性能极高！
        self.sleeping_heap: List[Tuple[float, int, Task]] = []

        # 3. 【阻塞等待池 (Waiting Set)】：存放正在等待事件触发或等待队列数据的阻塞任务
        self.waiting_tasks: set[Task] = set()

        # 4. 当前正在 CPU 上运行的任务
        self.current_task: Optional[Task] = None
        self.is_running = False
        self.cycle_count = 0 # 调度器事件循环运转总轮数

    def spawn(self, coro_or_func: Any, name: Optional[str] = None) -> Task:
        """
        【创建新任务】：相当于 Linux 的 fork() 或 pthread_create()
        将一个生成器包装为 Task，并直接放入就绪队列 (Ready Queue) 等待调度。
        """
        if not isinstance(coro_or_func, Generator):
            coro = coro_or_func()
        else:
            coro = coro_or_func

        task = Task(coro, name=name)
        self.ready_queue.append(task)
        return task

    # --------------------------------------------------------------------------
    # 🌟 核心调度主循环：事件循环 (The Event Loop)
    # --------------------------------------------------------------------------
    def run_until_idle(self):
        """
        【操作系统调度算法主循环】：
        只要系统中还有任务没跑完，就不停地按照以下 4 步循环运转：
        1. 检查定时器堆：将所有已经睡饱的任务，从睡眠堆移入就绪队列（唤醒）。
        2. 从就绪队列头部弹出一个任务（调度下一个幸运儿）。
        3. 上下文切换：让任务恢复执行一个时间片。
        4. 根据任务 yield 发出来的系统调用（Syscall），更新它的状态（休眠/阻塞/继续排队）。
        """
        self.is_running = True

        while self.is_running:
            now = time.time()

            # --- 步骤 1：唤醒所有到期的睡眠任务 ---
            while self.sleeping_heap and self.sleeping_heap[0][0] <= now:
                _, _, task = heapq.heappop(self.sleeping_heap)
                if task.state == TaskState.SLEEPING:
                    task.state = TaskState.READY
                    self.ready_queue.append(task) # 重新加入就绪队列排队

            # --- 步骤 2：检查就绪队列 ---
            if not self.ready_queue:
                # 如果就绪队列空了，且没有睡眠任务和等待任务，说明全部任务均已执行完毕！
                if not self.sleeping_heap and not self.waiting_tasks:
                    break

                # 如果就绪队列空了，但还有任务在 sleep：
                # 聪明地让 CPU 物理休眠到下一个最近的唤醒时刻（避免空转导致电脑风扇狂转 CPU 100%）
                if self.sleeping_heap:
                    next_wake_time = self.sleeping_heap[0][0]
                    sleep_duration = max(0.001, next_wake_time - time.time())
                    time.sleep(min(sleep_duration, 0.05))
                continue

            # --- 步骤 3：从就绪队列取出一个任务执行 ---
            task = self.ready_queue.popleft()
            self.current_task = task
            self.cycle_count += 1

            # --- 步骤 4：执行时间片与系统调用处理 ---
            try:
                # 恢复协程运行，并接收它交出来的系统调用对象 (Syscall)
                syscall = task.resume()

                if isinstance(syscall, SyscallSleep):
                    # 情况 A：任务申请睡眠 (sleep)
                    task.state = TaskState.SLEEPING
                    wake_time = time.time() + syscall.duration
                    task.wake_time = wake_time
                    # 压入最小堆：格式为 (唤醒时间戳, 任务ID, 任务对象)
                    heapq.heappush(self.sleeping_heap, (wake_time, task.tid, task))

                elif isinstance(syscall, SyscallYield):
                    # 情况 B：任务主动让出 CPU 时间片 (yield)
                    task.state = TaskState.READY
                    self.ready_queue.append(task) # 放到队尾，等下一轮再执行

                elif isinstance(syscall, SyscallWait):
                    # 情况 C：任务在等待资源阻塞 (wait)
                    task.state = TaskState.WAITING
                    self.waiting_tasks.add(task)

                else:
                    # 默认隐式让出：继续排队
                    task.state = TaskState.READY
                    self.ready_queue.append(task)

            except StopIteration as e:
                # 【正常终止】：生成器运行到 return 或末尾，触发 StopIteration
                task.state = TaskState.TERMINATED
                task.result = e.value
            except Exception as e:
                # 【异常崩溃】：记录报错并终止任务
                task.state = TaskState.TERMINATED
                task.exception = e
                print(f"❌ [调度器警告] 任务 [{task.name}] 发生未捕获异常: {e}")
            finally:
                self.current_task = None

        self.is_running = False

    def wake_task(self, task: Task):
        """【唤醒阻塞任务】：将一个因等待数据而阻塞的任务重新拉回就绪队列"""
        if task in self.waiting_tasks:
            self.waiting_tasks.remove(task)
            task.state = TaskState.READY
            self.ready_queue.append(task)


# ==============================================================================
# 🌟 第四部分：模拟操作系统内核系统调用 (Simulated System Calls)
# ==============================================================================
class SyscallSleep:
    """系统调用：申请非阻塞睡眠 duration 秒"""
    def __init__(self, duration: float):
        self.duration = duration


class SyscallYield:
    """系统调用：主动把当前 CPU 时间片让给其他排队的任务"""
    pass


class SyscallWait:
    """系统调用：挂起自己，等待被特定事件或数据唤醒"""
    pass


# 对外提供的极简协程辅助函数（零门槛使用）
def sleep(seconds: float):
    """协程版非阻塞休眠"""
    return (yield SyscallSleep(seconds))


def yield_cpu():
    """协程版主动让出 CPU"""
    return (yield SyscallYield())


# ==============================================================================
# 🌟 第五部分：进程间通信与同步原语 (IPC: AsyncQueue 异步队列)
# ==============================================================================
# 【操作系统经典模型：生产者-消费者问题】
#   如果队列满了，生产者必须暂停（阻塞）；
#   如果队列空了，消费者必须暂停（阻塞）；
#   当生产者放入新数据时，自动唤醒正在等待的消费者！
# ==============================================================================
class AsyncQueue:
    def __init__(self, scheduler: Scheduler, maxsize: int = 0):
        self.scheduler = scheduler
        self.maxsize = maxsize                      # 队列最大容量限制（0 表示无限制）
        self._queue = deque()                       # 存放真实数据的双端队列
        self._get_waiters: deque[Task] = deque()    # 等待取数据的消费者任务等待队列
        self._put_waiters: deque[Task] = deque()    # 等待放数据的生产者任务等待队列

    def put(self, item: Any):
        """【生产者放数据】：如果队列满了则自动挂起等待"""
        while self.maxsize > 0 and len(self._queue) >= self.maxsize:
            current = self.scheduler.current_task
            self._put_waiters.append(current)
            yield SyscallWait()

        self._queue.append(item)
        # 成功放入数据！如果有消费者在苦苦等待，立刻唤醒它
        if self._get_waiters:
            waiting_consumer = self._get_waiters.popleft()
            self.scheduler.wake_task(waiting_consumer)

    def get(self) -> Any:
        """【消费者取数据】：如果队列为空则自动挂起等待"""
        while not self._queue:
            current = self.scheduler.current_task
            self._get_waiters.append(current)
            yield SyscallWait()

        item = self._queue.popleft()
        # 成功取走数据，腾出了空位！如果有生产者在等待空位，立刻唤醒它
        if self._put_waiters:
            waiting_producer = self._put_waiters.popleft()
            self.scheduler.wake_task(waiting_producer)

        return item

    def qsize(self) -> int:
        """获取当前队列中已有物品的数量"""
        return len(self._queue)


class AsyncEvent:
    """
    【异步条件变量/事件同步】：
    类似操作系统的信号量与条件广播，允许多个任务挂起等待同一个信号的触发。
    """
    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self._is_set = False
        self._waiters: List[Task] = []

    def set(self):
        """触发事件：一口气唤醒所有正在等待该信号的任务 (Broadcast Wakeup)"""
        self._is_set = True
        while self._waiters:
            t = self._waiters.pop()
            self.scheduler.wake_task(t)

    def clear(self):
        """重置事件为未触发状态"""
        self._is_set = False

    def is_set(self) -> bool:
        return self._is_set

    def wait(self):
        """如果事件未触发，则将自己加入等待池并挂起"""
        while not self._is_set:
            self._waiters.append(self.scheduler.current_task)
            yield SyscallWait()
