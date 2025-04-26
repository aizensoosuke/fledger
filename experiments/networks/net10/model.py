from mergexp import *

net = Network('net10')

def makeNode(i: int):
    name = f"n{i}"
    return net.node(name, proc.cores>=1, memory.capacity>=mb(512))

sna = [makeNode(i) for i in range(10)]

link = net.connect(sna, capacity==mbps(1), latency==ms(10))

for i in range(10):
    suffix = str(i + 1)
    link[sna[i]].socket.addrs = ip4(f"10.0.0.{suffix}/24")

experiment(net)
