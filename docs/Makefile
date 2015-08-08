all:
	doxygen Doxyfile
	./list_undocumented_files doxygen_warn.log > undocumented_files.log
	./list_undocumented_members doxygen_warn.log > undocumented_members.log

clean:
	rm -f *.log
	rm -rf html
	rm -rf latex
