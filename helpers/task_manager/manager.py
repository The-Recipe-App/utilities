import asyncio
import time
import heapq
import signal
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Awaitable, Optional
from concurrent.futures import ThreadPoolExecutor
from database.main.core.schema import create_all_tables
from utilities.common.common_utility import debug_print

class TaskType(Enum):
    ASYNC = auto()
    THREAD = auto()

class TaskState(Enum):
    PENDING = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()

DEFAULT_TASKS = [
    {
        "func": create_all_tables,
        "task_type": TaskType.ASYNC,
        "timeout": 30,
        "run_immediately": True,
    },
]

@dataclass(order=True)
class ScheduledTask:
    next_run: float
    id: str = field(compare=False)
    func: Callable = field(compare=False)
    task_type: TaskType = field(compare=False)

    args: tuple = field(default_factory=tuple, compare=False)
    kwargs: dict = field(default_factory=dict, compare=False)

    interval: Optional[float] = field(default=None, compare=False)
    timeout: Optional[float] = field(default=None, compare=False)

    state: TaskState = field(default=TaskState.PENDING, compare=False)
    last_error: Optional[str] = field(default=None, compare=False)

    lock: asyncio.Lock = field(default_factory=asyncio.Lock, compare=False)

class TaskManager:
    def __init__(self, max_workers: int = 4):
        self._heap: list[ScheduledTask] = []
        self._tasks: dict[str, ScheduledTask] = {}

        self._shutdown = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def before_startup(self) -> None:
        for task in DEFAULT_TASKS:
            self.add_task(**task)

    async def start(self):
        debug_print("Starting Task Manager...")
        debug_print("Running before startup tasks...")
        await self.before_startup()
        debug_print("Before startup tasks complete.")
        if self._scheduler_task and not self._scheduler_task.done():
            return

        self._shutdown.clear()
        self._scheduler_task = asyncio.create_task(self._scheduler())

    async def shutdown(self):
        self._shutdown.set()

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        for task in self._tasks.values():
            if task.lock.locked():
                task.state = TaskState.CANCELLED

        self._executor.shutdown(wait=True, cancel_futures=True)


    def add_task(
        self,
        func: Callable,
        *,
        task_type: TaskType,
        interval: Optional[float] = None,
        timeout: Optional[float] = None,
        run_immediately: bool = True,
        run_once_and_forget: bool = False,   # run now, never reschedule
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())

        task = ScheduledTask(
            id=task_id,
            func=func,
            task_type=task_type,
            interval=interval,
            timeout=timeout,
            next_run=time.time() if run_immediately else float("inf"),
            args=args,
            kwargs=kwargs or {},
        )

        task._run_once_and_forget = run_once_and_forget

        self._tasks[task_id] = task

        if run_once_and_forget:
            asyncio.create_task(self._execute(task))

        else:
            heapq.heappush(self._heap, task)

        return task_id


    async def _scheduler(self):
        try:
            while not self._shutdown.is_set():
                if not self._heap:
                    await asyncio.sleep(0.5)
                    continue

                task = heapq.heappop(self._heap)

                if task.state in {TaskState.PAUSED, TaskState.CANCELLED}:
                    continue

                delay = task.next_run - time.time()
                if delay > 0:
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
                        break
                    except asyncio.TimeoutError:
                        pass

                await self._execute(task)

                if (
                    task.interval
                    and task.state not in {TaskState.CANCELLED, TaskState.FAILED}
                ):
                    task.next_run = time.time() + task.interval
                    heapq.heappush(self._heap, task)
        except asyncio.CancelledError:
            # graceful exit
            pass

    async def _execute(self, task: ScheduledTask):
        if task.lock.locked():
            return

        async with task.lock:
            task.state = TaskState.RUNNING
            try:
                if task.task_type == TaskType.ASYNC:
                    coro = task.func(*task.args, **task.kwargs)
                else:
                    loop = asyncio.get_running_loop()
                    coro = loop.run_in_executor(
                        self._executor,
                        lambda: task.func(*task.args, **task.kwargs),
                    )

                if task.timeout:
                    await asyncio.wait_for(coro, timeout=task.timeout)
                else:
                    await coro

                task.state = TaskState.COMPLETED

                # Only reschedule if it's NOT a run-once-and-forget task
                if (
                    task.interval
                    and not getattr(task, "_run_once_and_forget", False)
                    and task.state == TaskState.COMPLETED
                ):
                    task.next_run = time.time() + task.interval
                    heapq.heappush(self._heap, task)

            except asyncio.CancelledError as e:
                task.state = TaskState.CANCELLED
                debug_print(f"[TASK ERROR] {e!r}", color="red")
                raise

            except asyncio.TimeoutError as e:
                task.last_error = "Timeout exceeded"
                task.state = TaskState.FAILED
                debug_print(f"[TASK ERROR] {e!r}", color="red")

            except Exception as e:
                task.last_error = repr(e)
                task.state = TaskState.FAILED
                debug_print(f"[TASK ERROR] {e!r}", color="red")


    def pause(self, task_id: str):
        self._tasks[task_id].state = TaskState.PAUSED

    def resume(self, task_id: str):
        task = self._tasks[task_id]
        task.state = TaskState.PENDING
        task.next_run = time.time()
        heapq.heappush(self._heap, task)

    def cancel(self, task_id: str):
        self._tasks[task_id].state = TaskState.CANCELLED

    def restart(self, task_id: str):
        task = self._tasks[task_id]
        task.state = TaskState.PENDING
        task.last_error = None
        task.next_run = time.time()
        heapq.heappush(self._heap, task)

    def status(self):
        return {
            tid: {
                "state": task.state.name,
                "next_run": task.next_run,
                "last_error": task.last_error,
                "type": task.task_type.name,
            }
            for tid, task in self._tasks.items()
        }

task_manager = TaskManager()