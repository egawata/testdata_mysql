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
        'distinct_val', #  distinct values for each referenced tables/columns
                        #     $self->{distinct_val}{$table}{$column} = {
                        #       'value1'    => 1,
                        #       'value2'    => 1,
                        #     }
    ],
    ro      => [
        'inserted',     #  All inserted ids

        'defs',         #  Table definitions
                        #    $self->defs->{ $table_name } = (Test::HandyData::mysql::TableDef object)
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

my $DISTINCT_VAL_FETCH_LIMIT = 100;


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


=head2 insert($table, $valspec)

Inserts a record.

$valspec is a hashref which keys are columns' names in $table.


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
    my ($self, $table_name, $table_valspec) = @_;

    $table_valspec
        and $self->set_user_valspec($table_name, $table_valspec);

    return $self->process_table($table_name);
}



#  1つのテーブルに1レコードを追加する。
#  戻り値は、INSERT されたレコードの ID。
sub process_table {
    my ($self, $table, $tmpl_valspec) = @_;
    my $dbh = $self->dbh();

    #  条件の追加指定があればそれを読み込む
    $tmpl_valspec 
        and $self->add_user_valspec($table, $tmpl_valspec);


    my $table_def = $self->_table_def($table);

    #  ID 列の決定
    #  $exp_id : 事前に予測されるID。ユーザ指定があればその値、ユーザ指定がなく auto_increment であれば、AUTO_INCREMENT の値。
    #  $real_id : 実際に割り当てられたID。ユーザ指定があればその値になるが、auto_increment の場合は undef
    my ($exp_id, $real_id) = $self->get_id($table, $tmpl_valspec);
    debugf("id is ($exp_id, " . ($real_id || '(undef)') . ")");
    
    #  値を指定する必要のある列を抽出する
    my @colnames = $self->get_cols_requiring_value($table, $table_def->def);


    my $sql = $self->_make_insert_sql($table, \@colnames);
    my $sth = $dbh->prepare($sql);

    {
        my @values = ();

        for my $col (@colnames) {

            my $value;
        

            #  (1)PK、かつ値の指定が明示的にされている場合は、それを使う。
            if ( $table_def->is_pk($col) and $real_id ) {
                push @values, $real_id;
                next;
            }


            #  (2)外部キー制約の有無を確認(fk = 1 のときのみ)
            #  制約がある場合は、参照先テーブルにあるレコードの値を見て自身の値を決定する。
            if ( $self->fk ) {
                if ( my $referenced_table_col = $table_def->is_fk($col) ) {     #  ret = { table => 'table name, column => 'column name' }
                    $value = $self->determine_fk_value($table, $col, $referenced_table_col);
                }
            }


            #  (3)列に値決定のルールが設定されていればそれを使う
            if ( !defined($value) and my $valspec_col = $self->valspec()->{$table}{$col} ) {
                $value = $self->determine_value( $valspec_col );
            }
            

            #  (4)ルールが設定されていなければ、ランダムに値を決定する
            if ( !defined($value) ) {

                my $col_def = $table_def->column_def($col) 
                    or confess "No column def found. $col";

                my $type = $col_def->data_type;
                my $func = $VALUE_DEF_FUNC{$type}
                    or die "Type $type for $col not supported";
                
                $value = $self->$func($col_def, $exp_id);
                debugf("No rule found. Generates random value.($value)");

            }

            push @values, $value;

            if ( $table_def->is_pk($col) ) {
                $real_id = $value;
            }
        }

        debugf(sprintf( "INSERT INTO %s (%s) VALUES (%s)", $table, (join ',', @colnames), (join ',', @values) ));
        eval {
            $sth->execute(@values);
        };
        if ($@) {
            confess $@
        }
        
    }

    $sth->finish;

    my $inserted_id = $real_id || $dbh->{'mysql_insertid'};
    $self->add_inserted_id($table, $inserted_id);
   
    debugf("Inserted. table = $table, id = $inserted_id");
    
    return $inserted_id;
}


sub valspec {
    my ($self, $_valspec) = @_;

    if ( defined $_valspec ) {
        if ( ref $_valspec eq 'HASH' ) {
            $self->{_valspec} = $_valspec;
        }
        else {
            confess "Invalid valspec.";
        }
    }

    return $self->{_valspec} || {};
}


#  insert したレコードのID をテーブルごとに分類して登録する
sub add_inserted_id {
    my ($self, $table, $id) = @_;

    $table or confess "Missing table name";
    defined $id or confess "Missing ID. table = $table";

    $self->{inserted}{$table} ||= [];
    push @{ $self->{inserted}{$table} }, $id;
}



#  ルールにしたがって列値を決定する
sub determine_value {
    my ($self, $valspec_col) = @_;

    ref $valspec_col eq 'HASH'
        or confess "Invalid valspec type." . Dumper($valspec_col);

    my $value;

    if ( exists($valspec_col->{random}) ) {
        my $values = $valspec_col->{random};

        ref $values eq 'ARRAY'
            or confess "Value of 'random' is invalid. type = " . (ref $values);
        scalar(@$values) > 0
            or confess "Value of 'random' is an empty arrayref";

        my $ind = rand() * scalar(@$values);
        $value = $values->[$ind]; 

    }
    elsif ( exists($valspec_col->{fixval}) ) {
        my $fixval = $valspec_col->{fixval};
        ref $fixval eq ''
            or confess "Value of 'fixval' is invalid";

        $value = $fixval;
    }

    return $value;
}


#  特定テーブルの中に、特定の列値を持つレコードがあるか調べる。
#  戻り値は件数。
sub _value_exists_in_table_col {
    my ($self, $table, $col, $value) = @_;

    defined($table) and defined($col) and defined($value)
         or confess "Invalid args (requires 3 arg)";

    my $sth = $self->dbh()->prepare(qq{
        SELECT count(*) FROM $table WHERE $col = ?
    });
    $sth->execute($value);
    my $row = $sth->fetchrow_arrayref();

    return $row->[0];       #  count(*)
}


sub determine_fk_value {
    my ($self, $table, $col, $ref) = @_;

    my $value = undef;

    my $ref_table = $ref->{table};
    my $ref_col   = $ref->{column};

    $table and $col and $ref_table and $ref_col 
        or confess "Invalid args. (requires 3 args)";

    debugf("Column $col is a foreign key references $ref_table.$ref_col.");

    if ( $self->valspec()->{$table}{$col} ) {

        # 
        #  (1)値の決定方法に指定がある場合は、その方法により値を決定する。
        #
    
        if ( my $valspec_col = $self->valspec()->{$table}{$col} ) {
            $value = $self->determine_value( $valspec_col );
        }

        #  その値を持つレコードが参照先テーブルになければ、参照先にその値を持つレコードを新たに作成
        #  
        #  参照先テーブルの内容を毎回問い合わせるのは効率が悪いと考えたが、
        #  参照先のレコード数が大量の場合はメモリを浪費してしまうことも考慮し、
        #  あえて毎回問い合わせることにした。

        if ( 0 == $self->_value_exists_in_table_col($ref_table, $ref_col, $value) ) {     #  No record exists
            $self->process_table($ref_table, { $ref_col => $value });       #  レコード作成
            debugf("A referenced record created. id = $value");
        }

    }
    else {

        #
        #  (2)値の決定方法にユーザ指定がない場合
        #

        #  現在参照先テーブルにあるPK値を取得する
        #  結果は 
        #  $ref_ids => { (id1)  => 1, (id2) => 1, ... }  という形で取得される。
        my $ref_ids = $self->get_current_distinct_values($ref_table, $ref_col); 
    

        #  現存する参照先の値から1つ適当に選ぶ(1レコード以上ある場合)
        my @_ref_ids = keys %$ref_ids;
        if ( @_ref_ids ) {
            $value = $_ref_ids[ int(rand() * scalar(@_ref_ids)) ];
            debugf("Referenced record id = $value");

        }
        else {
            #  参照先にはまだレコードがないので、適当に作成   
            $value = $self->process_table($ref_table);      #  IDを指定していないので適当な値がIDになるはず
            $self->distinct_val()->{$ref_table}{$ref_col}{$value} = 1;            #  このIDを追加しておく
            debugf("Referenced record created. id = $value");
            
        }
    }

    return $value;

}


#  ID 列の値を決定する
#  ここでは exp_id(予測されるID列の値)と real_id(ID列の確定値)の2つを返している。
#  TODO: 現状、単一列、整数値にしか対応していない
sub get_id {
    my ($self, $table) = @_;

    my $table_def = $self->_table_def($table);
    my $pks = $table_def->pk_columns();

    my ($exp_id, $real_id);
    for my $col (@$pks) {   #  for each pk columns

        my $col_def = $table_def->column_def($col);
        

        #  呼び出し元から指定された条件があり、それによりPKの値を決定できるか確かめる。
        #  決定できる場合は $real_id にその値が入る。
        if (    $self->valspec()->{$table} 
                and $self->valspec()->{$table}{$col} 
                and $real_id = $self->determine_value( $self->valspec()->{$table}{$col} ) 
        ) 
        {

            #  呼び出し元から指定された条件があればそれに従う
            $exp_id = $real_id;

        }
        else {

            #  特に指定がない場合
            debugf("user value is not specified");

            if ( $col_def->is_auto_increment() ) {

                #  auto_increment が設定されていればそれに従う
                debugf("Column $col is an auto_increment");
                $exp_id = $table_def->get_auto_increment_value();
                
                #  real_id は insert 時に決まるため、undef のままにしておく。

            }
            else {
                #  なければランダムな値を生成する。
                debugf("Column $col is not an auto_increment");
                my $type = $col_def->data_type;
                my $size = $col_def->character_maximum_length;
                my $func = $VALUE_DEF_FUNC{$type}
                    or die "Type $type for $col not supported";
                
                $exp_id = $real_id = $self->$func($col_def);

            }
        }
    }

    return ($exp_id, $real_id);             
}



#  INSERT 実行時に値を指定する必要のある列のみ抽出する
sub get_cols_requiring_value {
    my ($self, $table) = @_;

    my $table_def = $self->_table_def($table);

    my @cols = ();
    for my $col ( $table_def->colnames ) {

        #  ユーザから値の指定がある場合は、必ずそれを使って指定する。
        #  なければ、列定義により、指定の要否を決める。
        unless ( defined( $self->valspec()->{$table}{$col} ) ) {

            my $col_def = $table_def->column_def($col);

            #  auto_increment 列は指定の必要なし
            next if $col_def->is_auto_increment;

            #  default 値が指定されている場合はそれを使用するので、指定の必要なし
            next if defined($col_def->column_default);

            #  NULL 値が認められているのであれば指定しない
            next if $col_def->is_nullable eq 'YES';

        }

        push @cols, $col;
    }

    return wantarray ? @cols : [ @cols ];
}


sub _table_def {
    my ($self, $table) = @_;

    $self->{_table_def}{$table} ||= Test::HandyData::mysql::TableDef->new( $self->dbh, $table );

    return $self->{_table_def}{$table};
}



sub _make_insert_sql {
    my ($self, $table_name, $colnames) = @_;

    my $cols = join ',', @$colnames;
    my $ph   = join ',', ('?') x scalar(@$colnames);
    my $sql  = "INSERT INTO $table_name ($cols) VALUES ($ph)";

    return $sql;
}


sub val_varchar {
    my ($self, $col_def, $exp_id) = @_;

    my $maxlen = $col_def->character_maximum_length;

    my $num_length = length($exp_id);
    my $colname = $col_def->name;
    my $colname_length = length($colname);

    if ( $colname_length + $num_length + 1 <= $maxlen ) {       #  (colname)_(num)
        return sprintf("%s_%d", $colname, $exp_id);
    }
    elsif ( $num_length + 1 <= $maxlen ) {                      #  (part_of_colname)_(num)
        my $part_of_colname = substr($colname, 0, $maxlen - $num_length - 1);
        return sprintf("%s_%d", $part_of_colname, $exp_id);
    }
    elsif ( $num_length == $maxlen ) {
        return $exp_id;
    }   
    else {                                                      #  random string
        $maxlen > $LENGTH_LIMIT_VARCHAR 
            or $maxlen = $LENGTH_LIMIT_VARCHAR;

        my $string = '';
        for (1 .. $maxlen) {
            $string .= $VARCHAR_LIST[ int( rand() * $COUNT_VARCHAR_LIST ) ];
        }

        return $string;
    }

}


sub val_tinyint {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_TINYINT_UNSIGNED) : int(rand() * $MAX_TINYINT_SIGNED);
}


sub val_smallint {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_SMALLINT_UNSIGNED) : int(rand() * $MAX_SMALLINT_SIGNED);
}

sub val_int {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_INT_UNSIGNED) : int(rand() * $MAX_INT_SIGNED);
}



sub val_numeric {
    my ($self, $col_def) = @_;

    my $precision = $col_def->numeric_precision;
    my $scale     = $col_def->numeric_scale;

    my $num = '';
    $num .= int(rand() * 10) for 1 .. $precision - $scale;
    $num .= '.';
    $num .= int(rand() * 10) for 1 .. $scale;

    return $num;
}


sub val_float {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? rand() * $MAX_INT_UNSIGNED : rand() * $MAX_INT_SIGNED;
}



sub val_datetime {
    my ($self, $col_def) = @_;

    return DateTime->from_epoch( epoch => time + rand() * 2 * $ONE_YEAR_SEC - $ONE_YEAR_SEC )->datetime();
}



=pod get_current_distinct_values($table, $col)

$table, $col で指定された表・列の値(distinct値)を一定個数取得する。


=cut

sub get_current_distinct_values {
    my ($self, $table, $col) = @_;

    my $current = $self->distinct_val()->{$table}{$col};

    if ( !defined $current or keys %$current == 0 ) {

        #  現存するレコードを確認
        my $sql = "SELECT DISTINCT $col FROM $table LIMIT $DISTINCT_VAL_FETCH_LIMIT";
        my $res = $self->dbh()->selectall_arrayref($sql);

        my %values = map { $_->[0] => 1 } @$res;

        $current = $self->distinct_val()->{$table}{$col} = { %values };
    }

    return $current;
}


=pod set_user_valspec($table_name, $valspec)

insert を実行するときの条件を設定する。
(これまで設定していた条件はクリアされる)


=cut

sub set_user_valspec {
    my ($self, $table, $table_valspec) = @_;

    #  前回の条件をクリア
    $self->valspec({});
    $self->distinct_val() or $self->distinct_val({}); 

    $self->add_user_valspec($table, $table_valspec);
}


=pod add_user_valspec

次回 insert を実行したときの条件を設定する。
(これまで設定していた条件に追加する)

=cut

sub add_user_valspec {
    my ($self, $table, $table_valspec) = @_;

    defined $table and $table =~ /^\w+$/
        or confess "Invalid table name [$table]";

    defined $table_valspec and ref $table_valspec eq 'HASH'
        or confess "Invalid user valspec. " . Dumper($table_valspec);


    for my $col (keys %$table_valspec) {
        
        my $_table = $table;
        my $_col   = $col;

        if ( $col =~ /^\w+\.\w+$/ ) {
            ($_table, $_col) = split '\.', $col;
        }

        my $val = $table_valspec->{$col};

        if ( ref $val eq 'ARRAY' ) {
            $self->valspec()->{$_table}{$_col}{random} = $val;
        }
        elsif ( ref $val eq 'HASH' ) {
            for (keys %$val) {
                $self->valspec()->{$_table}{$_col}{$_} = $val->{$_};
            }
        }
        elsif ( ref $val eq '' ) {
            $self->valspec()->{$_table}{$_col}{fixval} = $val;
        }
    }

}   



sub get_auto_increment_value {
    my ($self, $table_def) = @_;

    return $table_def->get_auto_increment_value();
}


sub delete_all {
    my ($self) = @_;

    my $dbh = $self->dbh();

    $dbh->do('SET FOREIGN_KEY_CHECKS = 0');

    for my $table ( keys %{ $self->inserted() } ) {
        my $pk_name = $self->_table_def($table)->pk_columns()->[0];
        my $sth = $dbh->prepare( qq{DELETE FROM $table WHERE $pk_name = ?} );
        for my $val ( @{ $self->inserted->{$table} } ) {
            $sth->execute($val);
            debugf("DELETE FROM $table WHERE $pk_name = $val");
        }
    }

    $dbh->do('SET FOREIGN_KEY_CHECKS = 1');

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

