run:
	go run main.go

watch:
	watch --interval 0.01 'top -l 1 | grep -E "^CPU|^Phys"'