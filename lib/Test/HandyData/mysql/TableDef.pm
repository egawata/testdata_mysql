package Test::HandyData::mysql::TableDef;

use strict;
use warnings;

use Carp;
use Data::Dumper;
use Log::Minimal;

use Test::HandyData::mysql::ColumnDef;



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

Sets/gets database handle.

=cut

sub dbh {
    my ($self, $dbh) = @_;

    defined $dbh
        and $self->{dbh} = $dbh;

    $self->{dbh} or confess "No dbh specified";
    return $self->{dbh};
}



=head2 table_name($name)

Sets/gets table name.

=cut

sub table_name {
    my ($self, $name) = @_;

    defined $name and $self->{table_name} = $name;

    defined $self->{table_name} or confess "No table name specified";
    return $self->{table_name};
}



=head2 colnames()

Gets all columns in this table.

=cut

sub colnames {
    my ($self) = @_;

    my $def = $self->def();
    return wantarray ? keys %$def : [ keys %$def ];
}


=head2 def()

Gets table definition.


=cut

sub def {
    my ($self) = @_;

    unless ( $self->{definition} ) {
        $self->{definition} = $self->_get_table_definition();
    }

    return +{ %{ $self->{definition} } };
}


=head2 constraint()

Gets table constaints


=cut

sub constraint {
    my ($self) = @_;

    unless ( $self->{constraint} ) {
        $self->{constraint} = $self->_get_table_constraint();
    }

    return +{ %{ $self->{constraint} } };
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

    $ret = {
        table   => 'table name',
        column  => 'column name'
    }

Otherwise, returns undef.


=cut

sub is_fk {
    my ($self, $colname) = @_;

    my $const_key = $self->constraint()->{$colname};

    if ( $const_key  
         and $const_key->{REFERENCED_TABLE_SCHEMA}
         and my $ref_table = $const_key->{REFERENCED_TABLE_NAME}
         and my $ref_col   = $const_key->{REFERENCED_COLUMN_NAME} ) {
        return { table => $ref_table, column => $ref_col };
    }
    else {
        return undef;
    }
}


=head2 pk_columns()

Gets column names of primary keys

=cut

sub pk_columns {
    my ($self) = @_;
   
    unless ( $self->{pk_columns} ) {
        my $constraint = $self->constraint;
        my @pk = ();        
        for my $col ( sort { $constraint->{$a}{ORDINAL_POSITION} <=> $constraint->{$b}{ORDINAL_POSITION} } keys %$constraint ) {
            push @pk, $col if $constraint->{$col}{CONSTRAINT_NAME} eq 'PRIMARY';
        }
        $self->{pk_columns} = [ @pk ];
    }

    return [ @{ $self->{pk_columns} } ];
}


=head2 column_def($column_name)

Gets column definition (ColumnDef object)


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


#
#  Get constaints from information_schema.key_column_usage.
#
# +-------------------------------+--------------+------+-----+---------+-------+
# | Field                         | Type         | Null | Key | Default | Extra |
# +-------------------------------+--------------+------+-----+---------+-------+
# | CONSTRAINT_CATALOG            | varchar(512) | NO   |     |         |       |
# | CONSTRAINT_SCHEMA             | varchar(64)  | NO   |     |         |       |
# | CONSTRAINT_NAME               | varchar(64)  | NO   |     |         |       |
# | TABLE_CATALOG                 | varchar(512) | NO   |     |         |       |
# | TABLE_SCHEMA                  | varchar(64)  | NO   |     |         |       |
# | TABLE_NAME                    | varchar(64)  | NO   |     |         |       |
# | COLUMN_NAME                   | varchar(64)  | NO   |     |         |       |
# | ORDINAL_POSITION              | bigint(10)   | NO   |     | 0       |       |
# | POSITION_IN_UNIQUE_CONSTRAINT | bigint(10)   | YES  |     | NULL    |       |
# | REFERENCED_TABLE_SCHEMA       | varchar(64)  | YES  |     | NULL    |       |
# | REFERENCED_TABLE_NAME         | varchar(64)  | YES  |     | NULL    |       |
# | REFERENCED_COLUMN_NAME        | varchar(64)  | YES  |     | NULL    |       |
# +-------------------------------+--------------+------+-----+---------+-------+
#
sub _get_table_constraint {

    my ($self, $table) = @_;

    my $sql = q{
        SELECT * 
        FROM information_schema.key_column_usage 
        WHERE table_schema = ? 
          AND table_name = ?
    };
    my $sth = $self->dbh()->prepare($sql);
    $sth->bind_param(1, $self->_dbname);
    $sth->bind_param(2, $self->table_name);
    $sth->execute();

    my $res = {};
    while ( my $ref = $sth->fetchrow_hashref ) {
        my $ref_uc = { map { uc($_) => $ref->{$_} } keys %$ref };
        my $column_name = $ref_uc->{COLUMN_NAME} 
            or confess "Failed to retrieve column name. " . Dumper($ref_uc);
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


