import time
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor


'''计算1+2+...+n
'''
def func(n):
    s = 0
    for i in range(n+1):
        s += i
    print(f"sum({n})={s}")
    return n, s

'''统计函数运行时间
'''
def get_duration(func_name, *args, **kwargs):
    print(f"{func_name} >>>>>>>>>>>>>>>>>>>>>")
    print(f"args: {args}")
    print(f"kwargs: {kwargs}")
    st = time.time()
    globals()[func_name](*args, **kwargs)
    print(f"TIME: {time.time() - st}\n")

'''单进程
'''
def single_process(nums):
    results = []
    for num in nums:
        result = func(num)
        results.append(result)
    for r in results:
        print(f"results({r[0]})={r[1]}")

'''多进程apply方法
'''
def multi_process_apply(parallel, nums):
    results = []
    with mp.Pool(processes=parallel) as pool:
        for num in nums:
            result = pool.apply(func, (num,))  # 同步，阻塞，直接返回计算结果
            results.append(result)
        for r in results:
            print(f"results({r[0]})={r[1]}")

'''多进程apply_async方法
'''
def multi_process_apply_async(parallel, nums, block=False):
    apply_results = []
    with mp.Pool(processes=parallel) as pool:
        for num in nums:
            apply_result = pool.apply_async(func, (num,))  # 异步，不阻塞，返回ApplyResult对象
            apply_results.append(apply_result)
        if block:  # 阻塞，等待所有进程结束
            pool.close()
            pool.join()
        for apply_result in apply_results:
            r = apply_result.get()  # 阻塞，按输入顺序返回计算结果
            print(f"results({r[0]})={r[1]}")

'''多进程map方法
'''
def multi_process_map(parallel, nums):
    with mp.Pool(processes=parallel) as pool:
        results = pool.map(func, nums)  # 同步，阻塞，按输入顺序返回所有进程的计算结果
        for r in results:
            print(f"results({r[0]})={r[1]}")

'''多进程map_async方法
'''
def multi_process_map_async(parallel, nums, block=False):
    with mp.Pool(processes=parallel) as pool:
        map_results = pool.map_async(func, nums)  # 异步，不阻塞，返回MapResult对象
        if block:  # 阻塞，等待所有进程结束
            pool.close()
            pool.join()
        map_results = map_results.get()  # 阻塞，按输入顺序返回计算结果
        for r in map_results:
            print(f"results({r[0]})={r[1]}")

'''多进程imap方法
'''
def multi_process_imap(parallel, nums, block=False):
    with mp.Pool(processes=parallel) as pool:
        result_iter = pool.imap(func, nums)  # 异步，不阻塞，返回迭代器
        if block:  # 阻塞，等待所有进程结束
            pool.close()
            pool.join()
        for r in result_iter:  # 阻塞，按输入顺序返回计算结果
            print(f"results({r[0]})={r[1]}")

'''多进程imap_unordered方法
'''
def multi_process_imap_unordered(parallel, nums, block=False):
    with mp.Pool(processes=parallel) as pool:
        result_iter = pool.imap_unordered(func, nums)  # 异步，不阻塞，返回迭代器
        if block:  # 阻塞，等待所有进程结束
            pool.close()
            pool.join()
        for r in result_iter:  # 阻塞，按进程结束顺序返回计算结果
            print(f"results({r[0]})={r[1]}")

'''多线程submit方法
'''
def multi_thread_submit(parallel, nums):
    futures = []
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        for num in nums:
            future = pool.submit(func, num)  # 异步，不阻塞，返回Future对象
            futures.append(future)
        for future in futures:
            r = future.result()  # 阻塞，按输入顺序返回计算结果
            print(f"results({r[0]})={r[1]}")

'''多线程map方法
'''
def multi_thread_map(parallel, nums):
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        result_iter = pool.map(func, nums)  # 异步，不阻塞，返回迭代器
        for r in result_iter:  # 阻塞，按输入顺序返回计算结果
            print(f"results({r[0]})={r[1]}")

if __name__ == "__main__":
    start, length = 10**8, 5
    nums = range(start, start+length)
    parallel = 2

    get_duration("single_process", nums)

    get_duration("multi_process_apply", parallel, nums)
    get_duration("multi_process_apply_async", parallel, nums, block=False)
    get_duration("multi_process_apply_async", parallel, nums, block=True)
    get_duration("multi_process_map", parallel, nums)
    get_duration("multi_process_map_async", parallel, nums, block=False)
    get_duration("multi_process_map_async", parallel, nums, block=True)
    get_duration("multi_process_imap", parallel, nums, block=False)
    get_duration("multi_process_imap", parallel, nums, block=True)
    get_duration("multi_process_imap_unordered", parallel, nums, block=False)
    get_duration("multi_process_imap_unordered", parallel, nums, block=True)

    get_duration("multi_thread_submit", parallel, nums)
    get_duration("multi_thread_map", parallel, nums)
