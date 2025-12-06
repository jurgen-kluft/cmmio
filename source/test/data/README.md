# Create a binary data file

On the terminal type the following to get a 32 MB binary file from /dev/mem:

```bash
time dd if=/dev/mem of=test1G.bin bs=1M count=32
```