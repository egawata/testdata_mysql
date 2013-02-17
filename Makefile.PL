use strict;
use inc::Module::Install;

name('Test-HandyData-mysql');
all_from('lib/Test/HandyData/mysql.pm');

requires('DBI');

test_requires('Test::More');
test_requires('Test::mysqld');

WriteAll;

