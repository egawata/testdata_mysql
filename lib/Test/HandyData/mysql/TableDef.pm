package Test::HandyData::mysql::TableDef;

use strict;
use warnings;

use Carp;
use Data::Dumper;
use Log::Minimal;

use Test::HandyData::mysql::ColumnDef;


=head1 NAME

Test::HandyData::mysql::TableDef - Manages table definition in mysql


=head1 VERSION

This documentation refers to Test::HandyData::mysql::TableDef version 0.0.1


=head1 SYNOPSIS

    use Test::HandyData::mysql::TableDef;
    use DBI;
   
    my $dbh = DBI->connect('dbi:mysql:dbname=testdb', 'username', 'password');     
    my $table_def = Test::HandyData::mysql::TableDef->new($dbh, 'table1');




=head1 DESCRIPTION

Mysql におけるテーブル定義を管理するクラス。


=head1 METHODS 

=head2 new($dbh, $table_name)

Constructor.

=cut

sub new {
    my ($inv, $dbh, $table_name) = @_;

    my $class = ref $inv || $inv;
    my $self = bless {}, $class;

    
    $self->dbh($dbh) if $dbh;
    $self->table_name($table_name) if $table_name;

    $self;
}




=head2 dbh($dbh)

Setter/getter for database handle generated by DBI.

=cut

sub dbh {
    my ($self, $dbh) = @_;

    defined $dbh
        and $self->{dbh} = $dbh;

    $self->{dbh} or confess "No dbh specified";
    return $self->{dbh};
}



=head2 table_name($name)

Setter/getter for table name.

=cut

sub table_name {
    my ($self, $name) = @_;

    defined $name and $self->{table_name} = $name;

    defined $self->{table_name} or confess "No table name specified";
    return $self->{table_name};
}



=head2 colnames()

Returns all columns in this table. If you have a table such as

    mysql> desc table1;
    +-------------+-------------+------+-----+---------+----------------+
    | Field       | Type        | Null | Key | Default | Extra          |
    +-------------+-------------+------+-----+---------+----------------+
    | id          | int(11)     | NO   | PRI | NULL    | auto_increment |
    | category_id | int(11)     | YES  | MUL | NULL    |                |
    | name        | varchar(20) | NO   |     | NULL    |                |
    | price       | int(11)     | NO   |     | NULL    |                |
    +-------------+-------------+------+-----+---------+----------------+

colnames() returns an arrayref containing 'id', 'category_id', 'name' and 'price'. Order won't be guaranteed.


=cut

sub colnames {
    my ($self) = @_;

    my $def = $self->def();
    return wantarray ? keys %$def : [ keys %$def ];
}


=head2 def()

Returns a table definition. It is a hashref which information is originally retrieved from information_schema.columns, which contains fields such as:

    +--------------------------+---------------------+------+-----+---------+-------+
    | Field                    | Type                | Null | Key | Default | Extra |
    +--------------------------+---------------------+------+-----+---------+-------+
    | TABLE_CATALOG            | varchar(512)        | NO   |     |         |       |
    | TABLE_SCHEMA             | varchar(64)         | NO   |     |         |       |
    | TABLE_NAME               | varchar(64)         | NO   |     |         |       |
    | COLUMN_NAME              | varchar(64)         | NO   |     |         |       |
    | ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | 0       |       |
    | COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
    | IS_NULLABLE              | varchar(3)          | NO   |     |         |       |
    | DATA_TYPE                | varchar(64)         | NO   |     |         |       |
    | CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
    | CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
    | NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
    | NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
    | CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
    | COLLATION_NAME           | varchar(32)         | YES  |     | NULL    |       |
    | COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
    | COLUMN_KEY               | varchar(3)          | NO   |     |         |       |
    | EXTRA                    | varchar(27)         | NO   |     |         |       |
    | PRIVILEGES               | varchar(80)         | NO   |     |         |       |
    | COLUMN_COMMENT           | varchar(1024)       | NO   |     |         |       |
    +--------------------------+---------------------+------+-----+---------+-------+

Table definition returned by def() is like the following:

    $ret = {
        'column_1'  => {
            TABLE_CATALOG   => 'def',
            TABLE_SCHEMA    => 'test',
            ...
        },
        'column_2'  => {
            TABLE_CATALOG   => 'def',
            TABLE_SCHEMA    => 'test',
            ...
        },
        ....
    }

Field names in the hashref are all converted to uppercase.


=cut

sub def {
    my ($self) = @_;

    unless ( $self->{definition} ) {
        $self->{definition} = $self->_get_table_definition();
    }
    return +{ %{ $self->{definition} } };
}


sub _fk {
    my ($self, $column_name) = @_;

    $self->{_fk} ||= {};

    unless ( $self->{_fk}{$column_name} ) {
        my $sth = $self->dbh->prepare(q{
            SELECT referenced_table_name,
                   referenced_column_name
             FROM information_schema.key_column_usage
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
              AND referenced_table_schema IS NOT NULL
              AND referenced_table_name IS NOT NULL
              AND referenced_column_name IS NOT NULL
        });
        $sth->execute( $self->_dbname, $self->table_name, $column_name )
            or confess "Failed to retrieve foreign key info (" 
                        . $self->table_name . ", $column_name)";
        
        my @res = ();
        while ( my $row = $sth->fetchrow_arrayref() ) {
            push @res, { table => $row->[0], column => $row->[1] };
        }

        $self->{_fk}{$column_name} = [ @res ];
    }

    return $self->{_fk}{$column_name};
}


=head2 is_pk($colname) 

Returns 1 if $colname is one of primary key columns. Otherwise returns 0.

=cut

sub is_pk {
    my ($self, $colname) = @_;

    return 
        ( grep { $_ eq $colname } @{ $self->pk_columns() } ) ? 1 : 0;
}


=head2 is_fk($colname)

If $colname is a foreign key, returns referenced table/column name like this:

    #  In case only one foreign key found
    $ret = {
        table   => 'table name',
        column  => 'column name'
    }
    
    #  In case multiple foreign keys found
    $ret = [
        { table => 'table1', column => 'column1' },
        { table => 'table2', column => 'column2' },
        { table => 'table3', column => 'column3' },
    ]

Otherwise, returns undef.

=cut

sub is_fk {
    my ($self, $colname) = @_;

    my $const_key = $self->_fk($colname);
    if ( @$const_key == 1 ) {
        return { %{ $const_key->[0] } };
    }
    elsif ( @$const_key == 0 ) {
        return undef;
    }
    else {
        return $const_key;
    }       

}


=head2 pk_columns()

Returns arrayref of column names of primary keys.


=cut

sub pk_columns {
    my ($self) = @_;
   
    unless ( $self->{pk_columns} ) {

        my $sth = $self->dbh->prepare(q{
            SELECT column_name FROM information_schema.key_column_usage
            WHERE constraint_name = 'PRIMARY'
              AND table_schema = ?
              AND table_name = ?
              ORDER BY ordinal_position
        });
        $sth->execute( $self->_dbname, $self->table_name )
            or confess "Failed to retrieve primary key info (" . $self->table_name . ")";
        
        my @pk = ();
        while ( my $row = $sth->fetchrow_arrayref() ) {
            push @pk, $row->[0];
        }

        $self->{pk_columns} = [ @pk ];
    }

    return [ @{ $self->{pk_columns} } ];
}


=head2 column_def($column_name)

Returns column definition (ColumnDef object)


=cut

sub column_def {
    my ($self, $column_name) = @_;

    defined $column_name
        or confess "Column name required.";

    $self->{column_def} ||= {};

    my $col_def = Test::HandyData::mysql::ColumnDef->new($column_name, $self->def->{$column_name});
    $self->{column_def}{$column_name} = $col_def;

    return $self->{column_def}{$column_name};    
}


=head2 get_auto_increment_value()

auto_increment が次に生成する値を取得する。


=cut

sub get_auto_increment_value {
    my ($self) = @_;

    my $table = $self->table_name;

    my $sql = q{SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_schema = ? AND table_name = ?};
    my $sth = $self->dbh()->prepare($sql);
    $sth->bind_param(1, $self->_dbname);
    $sth->bind_param(2, $self->table_name);
    $sth->execute();

    my $ref = $sth->fetchrow_hashref();
    my $ref_uc = { map { uc($_) => $ref->{$_} } keys %$ref };

    return $ref_uc->{AUTO_INCREMENT};
}


#
#  指定されたテーブルのテーブル定義を取得する。
#  結果は
#  　$res = {
#      (colname1) => (information_schema のレコード),
#      (colname2) => (  同上 ),
#      ..
#    }
#  のような形式で返す。 
#
sub _get_table_definition {
    my ($self, $table) = @_;

    my $sql = q{SELECT * FROM information_schema.columns WHERE table_schema = ? AND table_name = ?};
    my $sth = $self->dbh()->prepare($sql);
    $sth->bind_param(1, $self->_dbname);
    $sth->bind_param(2, $self->table_name);
    $sth->execute();
    my $res = {};
    while ( my $ref = $sth->fetchrow_hashref ) {

        #  取得された information_schema 結果のキーは環境により大文字、小文字の両方がありえるので、
        #  キー名はすべて大文字に変換する。
        my $ref_uc = { map { uc($_) => $ref->{$_} } keys %$ref };

        my $column_name = $ref_uc->{COLUMN_NAME} || confess "Failed to retrieve column name. " . Dumper($ref_uc);
        $res->{$column_name} = $ref_uc;
    } 

    return $res; 
}


sub _dbname {
    my ($self) = @_;

    unless ( $self->{_dbname} ) {
        my $res = $self->dbh()->selectall_arrayref('SELECT DATABASE()');
        $self->{_dbname} = $res->[0]->[0]
            or confess "Failed to get dbname";
    }
    
    return $self->{_dbname}; 
}



1;



__END__

=head1 DIAGNOSTICS

A list of every error and warning message that the module can generate
(even the ones that will "never happen"), with a full explanation of each 
problem, one or more likely causes, and any suggested remedies.
(See also  QUOTE \" " INCLUDETEXT "13_ErrorHandling" "XREF83683_Documenting_Errors_"\! Documenting Errors QUOTE \" " QUOTE " in Chapter "  in Chapter  INCLUDETEXT "13_ErrorHandling" "XREF40477__"\! 13.)


=head1 CONFIGURATION AND ENVIRONMENT

A full explanation of any configuration system(s) used by the module,
including the names and locations of any configuration files, and the
meaning of any environment variables or properties that can be set. These
descriptions must also include details of any configuration language used.
(also see  QUOTE \" " INCLUDETEXT "19_Miscellanea" "XREF40334_Configuration_Files_"\! Configuration Files QUOTE \" " QUOTE " in Chapter "  in Chapter  INCLUDETEXT "19_Miscellanea" "XREF55683__"\! 19.)


=head1 DEPENDENCIES

A list of all the other modules that this module relies upon, including any
restrictions on versions, and an indication whether these required modules are
part of the standard Perl distribution, part of the module's distribution,
or must be installed separately.


=head1 INCOMPATIBILITIES

A list of any modules that this module cannot be used in conjunction with.
This may be due to name conflicts in the interface, or competition for 
system or program resources, or due to internal limitations of Perl 
(for example, many modules that use source code filters are mutually 
incompatible).


=head1 BUGS AND LIMITATIONS

There are no known bugs in this module. 
Please report problems to Takashi Egawa.
Patches are welcome.

=head1 AUTHOR

Takashi Egawa


=head1 LICENCE AND COPYRIGHT

Copyright (c)2013 Takashi Egawa. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

