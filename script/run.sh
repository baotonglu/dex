#!/bin/bash

#ops
read=(100 0)
insert=(0 0)
update=(0 100)
delete=(0 0)
range=(0 0)

#fixed-op
readonly=(100 0 0 0 0)
updateonly=(0 0 100 0 0)

#exp
threads=(0 1 32 64 96 128)
mem_threads=(0 1 2 4)
cache=(0 64 128 256 512)
zipf=(0 0.5 0.7 0.9 0.99)
bulk=200
warmup=10
op=200

#other
correct=0
timebase=1
index=(0 1)
rpc=0
admit=1

# for t in {1..5}
# do
#     for c in 2 4
#     do
# 		for z in 1 4
# 		do
#             for i in 0 1
#             do
#                 filename="read-cache$c-zipf$z-index$i.txt"
#                 echo $filename
#                 ./restartMemc.sh
#                 sudo ./newbench 4 ${readonly[0]} ${readonly[1]} ${readonly[2]} ${readonly[3]} ${readonly[4]} ${threads[$t]} ${mem_threads[1]} ${cache[$c]} ${zipf[$z]} $bulk $warmup $op $correct $timebase $i $rpc $admit >> $filename 
#                 sleep 2
#             done
# 		done
# 	done
# done

for t in {1..5}
do
    for c in 2 4
    do
		for z in 1 4
		do
            for i in 0 1
            do
                filename="update-cache$c-zipf$z-index$i.txt"
                echo $filename
                ./restartMemc.sh
                sudo ./newbench 4 ${updateonly[0]} ${updateonly[1]} ${updateonly[2]} ${updateonly[3]} ${updateonly[4]} ${threads[$t]} ${mem_threads[1]} ${cache[$c]} ${zipf[$z]} $bulk $warmup $op $correct $timebase $i $rpc $admit >> $filename 
                sleep 2
            done
		done
	done
done