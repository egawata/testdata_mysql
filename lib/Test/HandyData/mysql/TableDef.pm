package Test::HandyData::mysql::TableDef;

use strict;
use warnings;

use Carp;


sub new {
    my ($inv, $params) = @_;

    my $class = ref $inv || $inv;
    my $self = bless {}, $class;

    $self;
}


=head2 table_name($name)

Sets/gets table name.

=cut

sub table_name {
    my ($self, $name) = @_;

    defined $name and $self->{table_name} = $name;

    return $self->{table_name};
}



=head2 def()

Gets table definition.


=cut

sub def {
    my ($self) = @_;

    unless ( $self->{definition} ) {
        $self->{definition} = $self->_get_table_definition();
    }

    return $self->{definition};
}


=head2 constraint()

Gets table constaints


=cut

sub constraint {
    my ($self) = @_;

    unless ( $self->{constraint} ) {
        $self->{constraint} = $self->_get_table_constraint();
    }

    return $self->{constraint};
}


=head2 pk_columns()

Gets column names of primary keys

=cut

sub pk_columns {
    my ($self) = @_;
   
    unless ( $self->{pk_columns} ) {
        my $constraint = $self->constraint;
        my @pk = ();        
        for my $col ( keys %$constraint) {
            push @pk, $col if $def->{$col}{CONSTRAINT_NAME} eq 'PRIMARY';
        }
        $self->{pk_columns} = [ @pk ];
    }
    
    return $self->{pk_columns}
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

    my $sql = "SELECT * FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
    my $sth = $self->dbh()->prepare($sql);
    $sth->bind_param(1, $self->dbname);
    $sth->bind_param(2, $table);
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
    $sth->bind_param(1, $self->dbname);
    $sth->bind_param(2, $table);
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




1;


