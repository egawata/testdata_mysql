package Test::HandyData::mysql;

use strict;
use warnings;

our $VERSION = '0.0.1';


use DBI;
use Data::Dumper;
use DateTime;
use Carp;
use Log::Minimal;
use Class::Accessor::Lite (
    new     => 1,
    rw      => [
        'dbh',          #  Database handle
        'fk',           #  1: Creates record on other table referenced by main table
        'nonull',       #  1: Assigns values to columns even if those default is NULL.      
        'cond_ref',     #  Special conditions for referenced tables.
    ],
    ro      => [
        'inserted',     #  All inserted ids

        'defs',         #  Table definitions
                        #    $self->defs->{ $table_name }{ $column_name } = {
                        #       COLUMN_NAME     => $column_name,
                        #       ...
                        #    }
                        
        'constraints',  #  Table constraints
    ],
);

use Test::HandyData::mysql::TableDef;


###############
#
#  Constants
#
###############

my $ONE_YEAR_SEC = 86400 * 365;

my @VARCHAR_LIST = ( 0..9, 'a'..'z', 'A'..'Z', '_' );
my $COUNT_VARCHAR_LIST = scalar @VARCHAR_LIST;

my $MAX_TINYINT_SIGNED       = 127;
my $MAX_TINYINT_UNSIGNED     = 255;
my $MAX_SMALLINT_SIGNED      = 32767;
my $MAX_SMALLINT_UNSIGNED    = 65535;
my $MAX_INT_SIGNED           = 2147483647;
my $MAX_INT_UNSIGNED         = 4294967295;

my $LENGTH_LIMIT_VARCHAR     = 10;

my %VALUE_DEF_FUNC = (
    char        => \&val_varchar,
    varchar     => \&val_varchar,
    tinyint     => \&val_tinyint,
    smallint    => \&val_smallint,
    int         => \&val_int,
    bigint      => \&val_int,
    numeric     => \&val_numeric,
    decimal     => \&val_numeric,
    float       => \&val_float,
    double      => \&val_float,
    datetime    => \&val_datetime,
    timestamp   => \&val_datetime,
    date        => \&val_datetime,
);


=head1 NAME
 
Test::HandyData::mysql - Generates handy test data for mysql 
 
 
=head1 VERSION
 
This documentation refers to Test::HandyData::mysql version 0.0.1
 
 
=head1 SYNOPSIS

    use DBI;
    use Test::HandyData::mysql;
   
    my $dbh = DBI->connect('dbi:mysql:test', 'user', 'pass');

    my $handy = Test::HandyData::mysql->new( fk => 1 );
    $handy->dbh($dbh);
    
    
    #  -- table definitions --
    #
    #  create table table1 (
    #      id           integer primary key auto_increment,
    #      group_id     interger not null,
    #      name         varchar(20) not null,
    #      price        integer not null,
    #      constraint foreign key group_id references group(id)
    #  );
    #  
    #  create table group (
    #      id           integer primary key,
    #      name         varchar(20) not null
    #  ); 
    #


    #
    #  Insert one row to 'table1'.
    #  'group_id', 'name' and 'price' will be random values.
    #  'group_id' refers to group(id), so the value will be selected one of ids in table 'group'.
    #  If table 'group' has no record, new record will be generated on 'group'. 
    #
    $handy->insert('table1');
    
        
    #
    #  Insert one row to 'table1' with name = 'Banana'
    #  group_id and price will be random values. 
    #
    $handy->insert('table1', { name => 'Banana' });
    
    
    #
    #  Insert one row to 'table1' with group_id one of 10, 20 or 30 (selected randomly)  
    #  If table 'group' has no record with id = 10, 20 nor 30, 
    #  3 records having those ids will be generated on 'group'.
    #
    $handy->insert('table1', { group_id => [ 10, 20, 30 ] });

  
=head1 DESCRIPTION

テスト用のデータを生成し、mysql のテーブルに INSERT します。

開発初期の動作確認時に、データベーステーブル上の列のうち、テストに関係のある列のみを指定し、他の列はどのような値でも構わないということがよくあります。しかし NOT NULL や FOREIGN KEY などの制約がある場合、関心のないデータ、さらには関心のない他のテーブル上のデータについてすべて指定しなければなりません。これは煩わしいことです。

このモジュールは、関心がある列以外のデータをランダムに生成し、テーブルへレコードを INSERT することを可能にします。また必要であれば、外部参照先のレコードも同時に生成します。


 
=head1 METHODS 
 

=cut 

=head2 new(%params)


=head2 dbh($dbh)

passes database handle


=head2 fk($bool)

creates records on other tables referred by foreign key columns in main table, if necessary.


=head2 nonull($bool)

assigns values to columns even if those defaults are NULL.


=head2 insert($table, $cond)

Inserts a record.

$cond is a hashref which keys are columns' names in $table.


=item colname => $scalar

specifies a value of 'colname'

    $handy->insert('table1', { id => 5 });      #  id is 5


=item colname => [ $val1, $val2, ... ]

value of 'colname' is decided as one of $val1, $val2, ... randomly.

    $handy->insert('table1', { id => [ 10, 20, 30 ] })      #  id is one of 10, 20, 30


=item colname => { random => [ $val1, $val2, ... ] }

verbose expression of above


=item colname => { fk => 1 }

creates records on other tables referred by foreign key columns in main table, if necessary. (Overrides object's "fk" attribute)


=item colname => { nonull => $bool }

assigns/doesn't assign value to the column even if its default is NULL. (Overrides object's "nonull" attribute)

    $handy->insert('table1', { group_id => { random => [ 10, 20, 30 ], fk => 1, nonull => 1 } });


=cut

sub insert {
    my ($self, $table_name, $table_cond) = @_;

    $self->set_user_cond($table_name, $table_cond);

    return $self->process_table($table_name, $table_cond);
}



#  1つのテーブルに1レコードを追加する。
#  戻り値は、INSERT されたレコードの ID。
sub process_table {
    my ($self, $table, $table_cond) = @_;
    my $dbh = $self->dbh();

    #  条件を読み込む
    #$self->parse_table_cond($table, $table_cond);

    my $def = $self->get_table_definition($table);
    my $constraint = $self->get_constraint($table);

    #  ID 列の決定
    my $id = $self->get_id($table);
    print "ID is $id\n";

    
    #  値を指定する必要のある列のみ抽出する
    my @colnames = $self->get_cols_requiring_value($table, $def);

    my $cols = join ',', @colnames;
    my $ph   = join ',', ('?') x scalar(@colnames);
    my $sql = "INSERT INTO $table ($cols) VALUES ($ph)";

    my $sth = $dbh->prepare($sql);

    {
        my @values = ();
        for my $key (@colnames) {

            my $type = $def->{$key}{DATA_TYPE};
            my $size = $def->{$key}{CHARACTER_MAXIMUM_LENGTH};
            my $opt  = $def->{$key}{COLUMN_TYPE};

            my $value;

            #  外部キー制約の有無を確認
            my $const_key = $constraint->{$key};
            my ($ref_table, $ref_col);
            if ( defined $const_key 
                and $const_key->{REFERENCED_TABLE_SCHEMA} 
                and $ref_table = $const_key->{REFERENCED_TABLE_NAME} 
                and $ref_col   = $const_key->{REFERENCED_COLUMN_NAME} ) 
            {

                my $ref_ids = $self->cond_ref()->{$table}{$key}{random}
                                || $self->get_current_ref_keys($ref_table, $ref_col); 
                
                if ( $self->cond()->{$table}{$key} ) {

                    #  値の指定がある場合は、その値を持つレコードを必要に応じて参照先に作る
                    $value = $self->determine_value( $self->cond()->{$table}{$key} );
                    
                    if ( ! grep { $_ eq $value } @$ref_ids ) {

                        $self->process_table($ref_table, { $ref_col => $value });       #  レコード作成
                        push @{ $self->cond_ref()->{$table}{$key}{random} }, $value;            #  このIDを追加しておく
                    }

                }
                else {

                    #  値の指定がないので、適当に参照先レコードを作成する

                    #  fk = 1 のときのみ参照先テーブルにレコードを追加する。
                    if ( $self->fk ) {
                        my $ref_keys = $self->process_table($ref_table);
                        $ref_ids = $self->get_current_ref_keys($ref_table, $ref_col);
                        if ( @$ref_ids ) {
                            $self->cond_ref()->{$table}{$key}{random} = [ @$ref_ids ];
                        }
                        else {
                            die "Something is wrong\n";
                        }
                    }
                    else {
                        #  do nothing
                    }
                }

            }


            #  列に値決定のルールが設定されていればそれを使う
            if ( !defined($value) ) {
                for ( $self->cond(), $self->cond_ref() ) {
                    $value = $self->determine_value( $_->{$table}{$key} );
                    defined($value) and last;
                }
            }
            

            #  ルールが設定されていなければ、ランダムに値を決定する
            if ( !defined($value) ) {
                my $func = $VALUE_DEF_FUNC{$type}
                    or die "Type $type for $key not supported";
                
                $value = $func->($size, $opt, $def->{$key});
            }

            push @values, $value;
        }

        $sth->execute(@values);
        
    }

    $sth->finish;

    my $inserted_id = $dbh->{'mysql_insertid'};
    $self->add_inserted_id($table, $inserted_id);
    
    return $inserted_id;
}


sub cond {
    my ($self, $_cond) = @_;

    defined $_cond and ref $_cond eq 'HASH'
        and $self->{_cond} = $_cond;

    return $self->{_cond} || {};
}


#  insert したレコードのID をテーブルごとに分類して登録する
sub add_inserted_id {
    my ($self, $table, $id) = @_;

    $self->{inserted}{$table} ||= [];
    push @{ $self->{inserted}{$table} }, $id;
}


#  ルールにしたがって列値を決定する
sub determine_value {
    my ($self, $cond_key) = @_;

    my $value;

    if ( defined($cond_key->{random}) ) {
        my $ind = rand() * @{ $cond_key->{random} };
        $value = $cond_key->{random}[$ind]; 
    }
    elsif ( exists($cond_key->{fixval}) ) {
        $value = $cond_key->{fixval};
    }

    return $value;
}


#  ID 列の値を決定する
#  TODO: 現状、単一列、整数値にしか対応していない
sub get_id {
    my ($self, $table) = @_;

    my $table_def = $self->table_def($table);
    my $pks = $table_def->pk_columns();

    my $id = undef;
    for my $col (@$pks) {
        debugf("key_column: $col");

        my $col_def = $table_def->column_def($col);
        

        #  呼び出し元から指定された条件があればそれに従う
        #  特に指定がない場合
        #  auto_increment が設定されていればそれに従う
        #  なければランダムな値を生成する。
        unless ( $self->cond()->{$table} and $self->cond()->{$table}{$col} and $id = $self->determine_value( $self->cond()->{$table}{$col} ) ) {

            debugf("user value is not specified");
            if ( $col_def->is_auto_increment() ) {
                debugf("Column $col is an auto_increment");
                $id = $self->get_auto_increment_value($table_def);

            }
            else {
                debugf("Column $col is not an auto_increment");
                my $type = $col_def->data_type;
                my $size = $col_def->character_maximum_length;
                my $func = $VALUE_DEF_FUNC{$type}
                    or die "Type $type for $col not supported";
                
                $id = $func->($size, undef, $col_def);

            }
        }
    }

    return $id;             
}


sub _is_auto_increment {
    my ($self, $table, $col) = @_;

    return
        ( $self->get_table_definition()->{$table}{$col}{EXTRA} =~ /auto_increment/ ) ? 1 : 0 ;
}


#  INSERT 実行時に値を指定する必要のある列のみ抽出する
sub get_cols_requiring_value {
    my ($self, $table, $def) = @_;

    return grep { 
        defined( $self->cond()->{$table}{$_} )
        or (
            $def->{$_}{EXTRA} !~ /auto_increment/           #  auto_increment 列でない
            and not defined($def->{$_}{COLUMN_DEFAULT})     #  default 値が指定されていない
            and $def->{$_}{IS_NULLABLE} eq 'NO'
        )
    } 
    grep { $_ !~ /^-/ }
    keys %$def;
}


sub dbname {
    my ($self) = @_;

    unless ( $self->{dbname} ) {
        my $res = $self->dbh()->selectall_arrayref('SELECT DATABASE()');
        $self->{dbname} = $res->[0]->[0]
            or confess "Failed to get dbname";
    }
    
    return $self->{dbname}; 
}


sub table_def {
    my ($self, $table) = @_;

    $self->{_table_def} ||= {};

    $self->{_table_def}{$table} ||= Test::HandyData::mysql::TableDef->new( $self->dbh, $table );

    return $self->{_table_def}{$table};
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
sub get_table_definition {
    my ($self, $table) = @_;

    $self->{defs} ||= {};

    unless ( $self->defs->{$table} ) {

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

        $self->defs->{$table} = { %$res };
    }

    return $self->defs->{$table};
}


sub get_constraint {
    my ($self, $table) = @_;

    my $sql = "SELECT * FROM information_schema.key_column_usage where table_schema = ? AND table_name = ?";
    my $sth = $self->dbh()->prepare($sql);
    $sth->bind_param(1, $self->dbname);
    $sth->bind_param(2, $table);
    $sth->execute();
    my $res = {};
    while ( my $ref = $sth->fetchrow_hashref ) {
        my $ref_uc = { map { uc($_) => $ref->{$_} } keys %$ref };
        my $column_name = $ref_uc->{COLUMN_NAME} || confess "Failed to retrieve column name. " . Dumper($ref_uc);
        $res->{$column_name} = $ref_uc;
    }
    
    return $res;  
}

sub val_varchar {
    my ($size) = @_;

    $size > $LENGTH_LIMIT_VARCHAR 
        or $size = $LENGTH_LIMIT_VARCHAR;

    my $string = '';
    for (1 .. $size) {
        $string .= $VARCHAR_LIST[ int( rand() * $COUNT_VARCHAR_LIST ) ];
    }

    return $string;
}


sub val_tinyint {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_TINYINT_UNSIGNED) : int(rand() * $MAX_TINYINT_SIGNED);
}

sub val_smallint {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_SMALLINT_UNSIGNED) : int(rand() * $MAX_SMALLINT_SIGNED);
}

sub val_int {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_INT_UNSIGNED) : int(rand() * $MAX_INT_SIGNED);
}


sub val_numeric {
    my ($size, $opt, $def) = @_;

    my $precision = $def->{NUMERIC_PRECISION};
    my $scale     = $def->{NUMERIC_SCALE};

    my $num = '';
    $num .= int(rand() * 10) for 1 .. $precision - $scale;
    $num .= '.';
    $num .= int(rand() * 10) for 1 .. $scale;

    return $num;
}


sub val_float {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? rand() * $MAX_INT_UNSIGNED : rand() * $MAX_INT_SIGNED;
}


sub val_datetime {
    return DateTime->from_epoch( epoch => time + rand() * 2 * $ONE_YEAR_SEC - $ONE_YEAR_SEC )->datetime();
}


sub get_current_ref_keys {
    my ($self, $table, $col) = @_;

    #  現存するレコードを確認
    my $ref_sql = "SELECT DISTINCT $col FROM $table LIMIT 100";
    my $ref_res = $self->dbh()->selectall_arrayref($ref_sql);

    return [ map { $_->[0] } @$ref_res ];
}


=pod parse_table_cond

(deprecated) 以降は set_user_cond を使うこと
=cut

sub parse_table_cond {
    my ($self, $table, $table_cond) = @_;

    $self->set_user_cond($table, $table_cond);
}


=pod set_user_cond($table_name, $cond)

次回 insert を実行したときの条件を設定する。
(これまで設定していた条件はクリアされる)


=cut

sub set_user_cond {
    my ($self, $table, $table_cond) = @_;

    debugf("start set_user_cond");

    return unless $table_cond and ref $table_cond eq 'HASH';

    debugf("Valid parameter");

    #  前回の条件をクリア
    $self->cond({});
    $self->cond_ref() or $self->cond_ref({}); 


    for my $col (keys %$table_cond) {
        
        my $_table = $table;
        my $_col   = $col;

        if ( $col =~ /^\w+\.\w+$/ ) {
            ($_table, $_col) = split '\.', $col;
        }

        my $val = $table_cond->{$col};

        if ( ref $val eq 'ARRAY' ) {
            $self->cond()->{$_table}{$_col}{random} = $val;
        }
        elsif ( ref $val eq 'HASH' ) {
            for (keys %$val) {
                $self->cond()->{$_table}{$_col}{$_} = $val->{$_};
            }
        }
        elsif ( ref $val eq '' ) {
            $self->cond()->{$_table}{$_col}{fixval} = $val;
        }
    }

    debugf("result cond : " . Dumper($self->cond()));
}   



sub get_auto_increment_value {
    my ($self, $table_def) = @_;

    return $table_def->get_auto_increment_value();
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
 
A list of known problems with the module, together with some indication
whether they are likely to be fixed in an upcoming release.
 
Also a list of restrictions on the features the module does provide: 
data types that cannot be handled, performance issues and the circumstances
in which they may arise, practical limitations on the size of data sets, 
special cases that are not (yet) handled, etc.
 
The initial template usually just has:
 
There are no known bugs in this module. 
Please report problems to <Maintainer name(s)>  (<contact address>)
Patches are welcome.
 
=head1 AUTHOR
 
Takashi Egawa  (egawa.takashi@gmail.com)
 
 
=head1 LICENCE AND COPYRIGHT
 
Copyright (c) 2012 Takashi Egawa (egawa.takashi@gmail.com). All rights reserved.
 
This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.
 
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

