# coding: utf-8
import os

helper_path = r"C:\Users\zhongying\qmt_signal_bridge\big_qmt_gateway_strategy_sample.py"

print("[QMT_LOADER] cwd=%s" % os.getcwd())
print("[QMT_LOADER] helper_path=%s" % helper_path)

f = open(helper_path, "r")
code = f.read()
f.close()

globals()["__file__"] = helper_path
exec(compile(code, helper_path, "exec"), globals())

print("[QMT_LOADER] loaded helper from %s" % helper_path)
