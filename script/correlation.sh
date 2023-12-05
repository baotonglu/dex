#!/bin/bash

# skewness
skewness=(0 0.5 0.7 0.9 0.99)
# cache capacity
cache=(0 64 128 256 512)
threads=(0 1 2 4 8 16 18)
admission=(0 0.01 0.2 0.4 0.6 0.8 1)
push=(0 0.1 0.2 0.4 0.6 0.8 0.9 0.99)
mem_threads=(0 1 2 4)

read=100
insert=0
update=0

# k specify skewness
# i specify cache
# j specify #threads
# m is push rate
# q specify admission
# mt means #thread in memory pool 
for i in 4
do
    for k in 4 1
    do
		for j in 1 5
		do
			for mt in 1
			do
				for m in {0..7}
				do 
					filename="0524-r$read-i$insert-u$update-s${skewness[$k]}-c${cache[$i]}-t${threads[$j]}-m${mem_threads[$mt]}-p${push[$m]}.txt"
					echo $filename
					#rm $filename
					for q in 6 5 4 3 2 1 0
					do
						./restartMemc.sh
						hog-machine.sh sudo ./benchmark-alex 2 $read $insert $update ${threads[$j]} ${cache[$i]} ${skewness[$k]} 200 100 0 ${push[$m]} ${admission[$q]} ${mem_threads[$mt]} 0 1 >> $filename 
						sleep 2
					done
				done
			done
		done
	done
done

