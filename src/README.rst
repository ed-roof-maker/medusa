
********
Medusa
********

Abstracts multi processing capabilities in the form of an inheritable super class.

.. contents:: 
Medusa can be imported into a project and be used as a super class when defining a subclass.
We use medusa to create multi worker job forking.
Can be used to -
   - download concurrently, push to a database concurrently, write to a filesystem concurrently
   - numeric data crunch concurrently
Medusa uses no third party libraries, and aims to only use what python gives us out of the box.



Installation
============
Install to local user bin under ~/.local/lib/python3.6, ~/.local/bin

.. code:: bash
    user:medusa/]$ pip3 wheel --wheel-dir=wheel $(realpath work)

.. code:: bash
    user:medusa/]$ pip3 install --user --no-index --find-links=wheel medusa
    user:medusa/]$ pip3 --list



Usage
=====
See examples/medusa-template.py
	


Use Pip Without Internet
========================
.. code:: bash
	$ pip3 install --no-index --find-links=<Custom Package Dir> <Package Name>


Setup environment variables in bashrc so you don't have to provide extra command line arguments.
.. code:: bash
	# Save these 2 variables in your profile 
	$ export PIP_NO_INDEX=true
	$ export PIP_FIND_LINKS=<Custom Package Dir>
	
	# Then run pip as usual
	$ pip install <package-name>



