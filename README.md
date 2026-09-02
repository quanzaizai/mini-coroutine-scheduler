# 🧵 C 语言微型用户态协程调度器 (mini-coroutine-scheduler)

所属专业课：《操作系统》《并发系统设计》

---

## 📖 工程目录结构解析

```text
mini-coroutine-scheduler/
├── coroutine.h    # 📌【核心头文件】：定义协程状态枚举、上下文结构体与调度器 API
├── coroutine.c    # 🔨【调度器内核】：基于 setjmp/longjmp 或 ucontext 实现用户态上下文切换
├── main.c         # 💡【实战演示】：经典生产者-消费者多协程协作运行范例
├── Makefile       # ⚙️【一键编译脚本】：自动化编译指令
└── README.md      # 📘【项目文档】：协程原理与控制流状态机解析
```

---

## 🗂️ 本项目所有文件详细功能与角色速查

| 所在目录/文件名 | 承担功能与底层作用 |
| :--- | :--- |
| [`coroutine.h`](./coroutine.h) | **协程接口定义**：定义 `Coroutine` 状态（READY就绪/RUNNING运行/SUSPEND挂起/DEAD销毁）及调度器结构 |
| [`coroutine.c`](./coroutine.c) | **调度器底层实现**：管理用户态独立调用栈空间，实现协程让出 (`yield`) 与恢复 (`resume`) 的上下文保存与还原 |
| [`main.c`](./main.c) | **多协程协作演示**：演示 3 个协程在单线程内非抢占式交替执行，模拟经典的协程生产者-消费者管道 |

---

## 🛠️ 构建与测试运行

```bash
make run   # 编译并运行微型协程调度器演示
```
