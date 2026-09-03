# 🧵 微型用户态协程调度器与事件循环 (mini-coroutine-scheduler)

所属专业课：《操作系统》《并发系统设计》《异步 I/O 与事件驱动架构》

> 🌟 **【零基础自学必读】**：想知道为什么 Node.js / Go 协程并发那么高？什么是任务控制块 TCB？进程五态如何用代码流转？为什么定时器必须用最小二叉堆？请先阅读保姆级设计手册：  
> 👉 **[📘 零基础工程架构与事件循环全景指南 (ARCHITECTURE_AND_DESIGN.md)](./ARCHITECTURE_AND_DESIGN.md)**

---

## 📖 工程目录结构解析

```text
mini-coroutine-scheduler/
├── ARCHITECTURE_AND_DESIGN.md  # 🌟 零基础架构设计与调度内核全景指南
├── scheduler.py                # 🔨 核心调度内核 (Task/TCB, Scheduler, AsyncQueue, AsyncEvent)
├── test_scheduler.py           # 🧪 自动化测试套件 (验证状态机/定时器/异步队列/事件广播)
├── examples/
│   └── producer_consumer.py    # 💡 经典多协程协作实战演示范例
├── pyproject.toml              # ⚙️ 现代标准项目元数据配置文件
├── .gitignore                  # Git 忽略配置
└── README.md                   # 本说明文档
```

---

## 🗂️ 本项目所有文件详细功能与角色速查

| 所在目录/文件名 | 承担功能与底层作用 |
| :--- | :--- |
| [`ARCHITECTURE_AND_DESIGN.md`](./ARCHITECTURE_AND_DESIGN.md) | **零基础保姆级手册**：解密协程原理、状态流转图解、最小堆定时器算法与五步通关卡 |
| [`scheduler.py`](./scheduler.py) | **微型调度器内核**：实现任务控制块 `Task`、就绪队列、最小堆睡眠定时器与协作式事件循环 |
| [`test_scheduler.py`](./test_scheduler.py) | **自动化单元测试**：验证五态转换、时间片让出、定时器精度、异步阻塞队列与广播事件 |
| [`examples/producer_consumer.py`](./examples/producer_consumer.py) | **多协程协作实战**：演示单线程内生产者与消费者协程通过 `AsyncQueue` 协作交替执行 |

---

## 🛠️ 构建与测试运行

本项目采用 Python 3 标准库实现，零外部依赖：

```bash
# 运行全部 4 项操作系统调度与同步自动化测试
python3 test_scheduler.py

# 运行经典生产者-消费者协程协作实战演示
python3 examples/producer_consumer.py
```
