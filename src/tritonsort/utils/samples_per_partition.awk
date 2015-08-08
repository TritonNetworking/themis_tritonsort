/Partition/ { sums[$4] += $6 }
END {for (i in sums) printf "%06d = %d\n",i,sums[i]}
