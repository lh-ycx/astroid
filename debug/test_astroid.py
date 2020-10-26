import os
import subprocess
from subprocess import Popen, PIPE
from threading import Timer


input_dir = "..\\..\\chengxu\\util\\marked_scripts\\"


def debug_print(*args, **kwargs):
    print("debug:", *args, **kwargs)


def run(cmd, timeout_sec):
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    timer = Timer(timeout_sec, proc.kill)
    try:
        timer.start()
        stdout, stderr = proc.communicate()
    finally:
        timer.cancel()
    return proc, stdout, stderr

if __name__ == '__main__':
    tot, sus = 0, 0
    for root, dirs, files in os.walk(input_dir, topdown=False):
        for name in files:
            if name[-3:] == ".py":
                print(f"test {name}")
                proc, stdout, stderr = run(f'python main.py {os.path.join(root, name)}', 10)
                return_code = proc.returncode
                stdout_output = stdout.decode()
                stderr_output = stderr.decode()
                debug_print('the status code is:', return_code)
                if return_code == 0:
                    sus += 1
                    debug_print(stdout_output)
                else:
                    debug_print(stderr_output)
                tot += 1
    print(f"testing results: {sus}/{tot}")