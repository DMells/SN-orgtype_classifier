import subprocess

# p = subprocess.Popen(["cd", "orgtype-classifier"], stdout=subprocess.PIPE)
# output = subprocess.call(["python server.py model.pkl.gz"], stdin=p.stdout)
# p.wait()
# cmd = "bash python server.py model.pkl.gz"
# cmd = ['bash', '-e','python server.py model.pkl.gz']
cmd = ["open -a Terminal ."]
# cmd = ['python server.py model.pkl.gz']
p = subprocess.Popen(cmd, shell=True, cwd=r'orgtype-classifier', stdout=PIPE)
p2 = subprocess.Popen()
p.wait()
