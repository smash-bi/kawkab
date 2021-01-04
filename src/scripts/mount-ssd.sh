#!/bin/bash

size="40g"

for i in $(seq 1 4); do
  if [ -e "/dev/nvme${i}n1" ]
  then
    echo "Mounting /dev/nvme${i}n1 to /ssd${i}"
    sudo mkfs.ext4 -E nodiscard /dev/nvme${i}n1 ${size}
    sudo mkdir -p /ssd${i}
    sudo mount -o discard /dev/nvme${i}n1 "/ssd${i}"
		sudo chown -R ubuntu /ssd1
    lsblk
  fi
done

