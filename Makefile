.PHONY: test doc site clean dist

test:
	PYTHONPATH=. `which python` corduroy/tests/__init__.py

doc: clean
	`which python` ./doc/build/spanx.py standalone

site: clean
	`which python` ./doc/build/spanx.py

clean:
	rm -f MANIFEST doc/guide.html doc/reference.html doc/readme.html \
      doc/index.html doc/guide/index.html doc/ref/index.html
	mkdir -p doc/guide && rmdir doc/guide
	mkdir -p doc/ref && rmdir doc/ref

dist: doc
	`which python` setup.py sdist