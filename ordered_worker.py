"""Bounded, ordered blocking work without holding an event-loop thread on a lock."""

import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor


class OrderedWorker:
    """One worker thread with bounded admission and cancellation-safe draining.

    Cancelling a waiter does not cancel an already accepted database operation.
    Its slot remains occupied until the operation really finishes. Closing rejects
    new work, drains accepted operations, then runs a finalizer on the same lane.
    """

    def __init__(self, name, max_pending=32):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._slots = asyncio.Semaphore(max_pending)
        self._pending = set()
        self._closing = False
        self._closed = False
        self._close_task = None
        self.max_pending = max_pending
        self.waiting = 0
        self.completed = 0
        self.failed = 0

    def snapshot(self):
        return {
            "pending": len(self._pending),
            "waiting": self.waiting,
            "limit": self.max_pending,
            "completed": self.completed,
            "failed": self.failed,
            "closing": self._closing,
            "closed": self._closed,
        }

    async def run(self, function, *args, **kwargs):
        if self._closing:
            raise RuntimeError("Observer worker is closing")
        self.waiting += 1
        try:
            await self._slots.acquire()
        finally:
            self.waiting -= 1
        if self._closing:
            self._slots.release()
            raise RuntimeError("Observer worker is closing")
        context = contextvars.copy_context()
        loop = asyncio.get_running_loop()
        try:
            future = loop.run_in_executor(
                self._executor, lambda: context.run(function, *args, **kwargs)
            )
        except BaseException:
            self._slots.release()
            raise
        self._pending.add(future)

        def finished(done):
            if done not in self._pending:
                return
            self._pending.discard(done)
            self._slots.release()
            self.completed += 1
            # Also retrieve failures when the caller has already been cancelled.
            if not done.cancelled() and done.exception() is not None:
                self.failed += 1

        future.add_done_callback(finished)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # Do not release an enclosing review/admission lock before an accepted
            # transaction really finishes. Stop flags are set on the loop first.
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    continue
                except BaseException:  # noqa: BLE001 - Preserve cancellation after draining any accepted failure.
                    break
            raise
        finally:
            # shield() can return an already-done future without another loop turn.
            # Account/release here too; its queued callback is idempotent.
            if future.done():
                finished(future)

    async def close(self, finalizer=None):
        if self._close_task is None:
            self._closing = True

            async def finish():
                try:
                    if self._pending:
                        await asyncio.gather(
                            *(asyncio.shield(f) for f in tuple(self._pending)),
                            return_exceptions=True,
                        )
                    if finalizer is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            self._executor, finalizer
                        )
                finally:
                    await asyncio.to_thread(self._executor.shutdown, wait=True)
                    self._closed = True

            self._close_task = asyncio.create_task(finish())
        await asyncio.shield(self._close_task)
