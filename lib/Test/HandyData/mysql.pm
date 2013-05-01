package Test::HandyData::mysql;

use strict;
use warnings;

our $VERSION = '0.0.1';


#  precision and scale of float value.
#  They may be changed from outside this module.
our $FLOAT_PRECISION = 4;
our $FLOAT_SCALE     = 2;

our $DISTINCT_VAL_FETCH_LIMIT = 100;

our $RANGE_YEAR_YEAR = 20;
our $RANGE_YEAR_DATETIME = 2;


use DBI;
use Data::Dumper;
use DateTime;
use Carp;
use Log::Minimal;
use SQL::Maker;
use Class::Accessor::Lite (
    new     => 1,
    rw      => [
        'dbh',          #  Database handle
        'fk',           #  1: Creates record on other table referenced by main table
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

my $LENGTH_LIMIT_VARCHAR     = 20;

my %VALUE_DEF_FUNC = (
    char        => \&_val_varchar,
    varchar     => \&_val_varchar,
    text        => \&_val_varchar,
    tinyint     => \&_val_tinyint,
    smallint    => \&_val_smallint,
    int         => \&_val_int,
    integer     => \&_val_int,
    bigint      => \&_val_int,
    numeric     => \&_val_numeric,
    decimal     => \&_val_numeric,
    float       => \&_val_float,
    double      => \&_val_float,
    datetime    => \&_val_datetime,
    timestamp   => \&_val_datetime,
    date        => \&_val_datetime,
    year        => \&_val_year,
);


=head1 NAME

Test::HandyData::mysql - Generates test data for mysql easily.


=head1 VERSION

This documentation refers to Test::HandyData::mysql version 0.0.1


=head1 SYNOPSIS

    use DBI;
    use Test::HandyData::mysql;
       
    my $dbh = DBI->connect('dbi:mysql:test', 'user', 'pass');
    
    my $hd = Test::HandyData::mysql->new( fk => 1 );
    $hd->dbh($dbh);
     
    
    #  -- table definitions --
    #
    #  create table category (
    #      id           integer primary key,
    #      name         varchar(20) not null
    #  ); 
    #
    #  create table item (
    #      id           integer primary key auto_increment,
    #      category_id  interger not null,
    #      name         varchar(20) not null,
    #      price        integer not null,
    #      constraint foreign key (category_id) references category(id)
    #  );
    
    
    #  1.
    #  Insert one row to 'table1'.
    #  'category_id', 'name' and 'price' will be random values.
    #  table1.group_id refers to group.id, so the value will be selected one of values in group.id.
    #  If table 'group' has no record, new record will be added to 'group'. 
    
    my $id = $hd->insert('table1');
    
    #  Result example:
    #  [item]
    #           id: 1
    #  category_id: 497364651
    #         name: name_1
    #        price: 597348646
    #
    #  [category]
    #           id: 497364651
    #         name: name_497364651
    #    
    
    print "ID: $id\n";      #  'ID: 1'
    
        
    #  2.
    #  Insert one row to 'item' with name = 'Banana'
    #  category_id and price will be random values. 
    
    $id = $hd->insert('item', { name => 'Banana' });  #  Maybe $id == 2
    
    #  Result example:
    #  [item]
    #           id: 2
    #  category_id: 497364651
    #         name: Banana
    #        price: 337640949
    #
    #  [category]
    #           id: 497364651
    #         name: name_497364651
    
     
    #  3.      
    #  Insert one row to 'item' with category_id one of 10, 20 or 30 (selected randomly)  
    #  If table 'category' has no record with id = 10, 20 nor 30, 
    #  a record having one of those ids will be generated on 'category'.
    
    $hd->insert('item', { category_id => [ 10, 20, 30 ] });
    
    #  Result example:
    #  [item] 
    #           id: 3
    #  category_id: 20
    #         name: name_3
    #        price: 587323402
    #
    #  [category]
    #           id: 20
    #         name: name_20
    
   
    #  Delete all records inserted by $hd
    
    $hd->delete_all();     


=head1 DESCRIPTION

This module generates test data and insert it into mysql tables. You only have to specify values of columns you're really interested in. Other necessary values are generated automatically.

When we test our product, sometimes we need to create test records, but generating them is a tedious task. We should consider many constraints (not null, foreign key, etc.) and set values to many columns in many tables, even if we want to do small tests, are interested in only a few columns and don't want to care about others. Maybe this module get rid of much of those unnecessary task.


=head1 METHODS 


=head2 new(%params)

Constructor.


=head2 dbh($dbh)

set a database handle


=head2 fk($flag)

also creates records on other tables referred by foreign key columns in main table, if necessary. 

Default is 0 (doesn't add records to other tables), so if you want to use this functionality, you need to specify 1 explicitly.


=cut

sub _sql_maker {
    my ($self) = @_;
    $self->{_sql_maker} ||= SQL::Maker->new( driver => 'mysql' );
    return $self->{_sql_maker};
}


#  distinct values for each referenced tables/columns
#     $self->{_distinct_val}{$table}{$column} = {
#       'value1'    => 1,
#       'value2'    => 1,
#     }
sub _distinct_val {
    my ($self) = @_;

    $self->{_distinct_val} ||= {};

    return $self->{_distinct_val};
}


=head2 insert($table_name, $valspec)

Inserts a record to a table named $table_name.

You can specify values of each column(s) with $valspec, a hashref which keys are columns' names in $table_name.

    $hd->insert('table1', {
        id      => 5,
        price   => 300
    });

=head3 format

=over 4

=item * colname => $scalar

specifies a value of 'colname'

    $handy->insert('table1', { id => 5 });      #  id will become 5


=item * colname => [ $val1, $val2, ... ]

value of 'colname' is decided as one of $val1, $val2, ... randomly.

    $handy->insert('table1', { id => [ 10, 20, 30 ] })      #  id will become one of 10, 20 or 30


=item * colname => { random => [ $val1, $val2, ... ] }

verbose expression of above

=back

=head3 column name

If you want to specify values of other tables (maybe referenced by foreign key), join table name and column name with dot(.)

    $valspec = {
        column1                  => 50,           #  Column in the same table
        'another_table.column2'  => [10, 20, 30]  #  Column in referenced table
    }

=head3 return value

Returns a value of primary key. (Only when primary key exists and it contains only a single column. Otherwise returns undef.)

=cut

sub insert {
    my ($self, $table_name, $table_valspec) = @_;

    $table_valspec
        and $self->_set_user_valspec($table_name, $table_valspec);

    return $self->process_table($table_name);
}



sub process_table {
    my ($self, $table, $tmpl_valspec) = @_;
    my $dbh = $self->dbh();

    #  Reads an additional spec
    $tmpl_valspec 
        and $self->_add_user_valspec($table, $tmpl_valspec);


    my $table_def = $self->_table_def($table);

    #  Determines ID value.
    #  $exp_id  : Expected ID. User specified value if specified, or auto_increment value if auto_increment column.
    #  $real_id : User specified value if specified. Otherwise undef.
    my ($exp_id, $real_id) = $self->get_id($table, $tmpl_valspec);
    debugf("id is (" . ($exp_id || '(undef)') . ", " . ($real_id || '(undef)') . ")");
    

    #  columns to which we need to specify values.
    my @colnames = $self->get_cols_requiring_value($table, $table_def->def);


    my %values = ();

    for my $col (@colnames) {

        my $value;
    
        #  (1)Primary key, and a value is specified by user.
        if ( $table_def->is_pk($col) and $real_id ) {
            $values{$col} = $real_id;
            next;
        }

        my $col_def = $table_def->column_def($col) 
            or confess "No column def found. $col";


        #  (2)If $self->fk = 1 and the column is a foreign key.
        if ( $self->fk ) {
            if ( my $referenced_table_col = $table_def->is_fk($col) ) {     #  ret = { table => 'table name, column => 'column name' }
                if ( ref $referenced_table_col eq 'HASH' ) { 
                    $value = $self->determine_fk_value($table, $col, $referenced_table_col);
                }
                else {
                    warn "Currently only one foreign key per column is supported.";
                }
            }
        }

        #  (2.5)If column default is available, use it.
        if ( !defined($value) and defined($col_def->column_default) ) {
            $value = $col_def->column_default;
        }            


        #  (3)If user specified a value, use it.
        if ( !defined($value) and my $valspec_col = $self->_valspec()->{$table}{$col} ) {
            $value = $self->determine_value( $valspec_col );
        }
        

        #  (4)Otherwise, decide a value randomly.
        if ( !defined($value) ) {

            my $type = $col_def->data_type;
            my $func = $VALUE_DEF_FUNC{$type};

            #  If this data type is not supported, leave it NULL.
            unless ($func) {
                warn "Type $type for $col is not supported.";
                next;
            }
            
            $value = $self->$func($col_def, $exp_id);
            debugf("No rule found. Generates random value.($value)");

        }

        $values{$col} = $value;

        if ( $table_def->is_pk($col) ) {
            $real_id = $value;
        }
    }

    eval {
        my ($sql, @bind) = $self->_sql_maker->insert($table, \%values);
        debugf($sql .  ", binds [" . (join ', ', @bind) . "]");

        my $sth = $dbh->prepare($sql);
        $sth->execute(@bind);
        $sth->finish;
    };
    if ($@) {
        confess $@
    }
        
    my $inserted_id = undef;
   
    
    #  Handles PK value only when the table has single pk column.
    if ( @{ $table_def->pk_columns() } == 1 ) {
        $inserted_id = $real_id || $dbh->{'mysql_insertid'};
        $self->add_inserted_id($table, $inserted_id);
   
        debugf("Inserted. table = $table, id = $inserted_id");
    }
    
    return $inserted_id;
}


sub _valspec {
    my ($self, $_valspec) = @_;

    if ( defined $_valspec ) {
        if ( ref $_valspec eq 'HASH' ) {
            $self->{_valspec} = $_valspec;
        }
        else {
            confess "Invalid valspec.";
        }
    }

    $self->{_valspec} ||= {};
    return $self->{_valspec};
}


#  Records an ID of inserted record.
sub add_inserted_id {
    my ($self, $table, $id) = @_;

    $table or confess "Missing table name";
    defined $id or confess "Missing ID. table = $table";

    $self->{inserted}{$table} ||= [];
    push @{ $self->{inserted}{$table} }, $id;
}



#  Determine a value of column according to (user-specified) rules.
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


#  Check if a record with specified column value exists.
#  Return value is a count of record(s).
sub _value_exists_in_table_col {
    my ($self, $table, $col, $value) = @_;

    defined($table) and defined($col) and defined($value)
         or confess "Invalid args (requires 3 arg)";

    my ($sql, @binds) = $self->_sql_maker->select( $table, [\'count(*)'], { $col => $value } );
    my $sth = $self->dbh()->prepare($sql);
    $sth->execute(@binds);
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

    if ( my $valspec_col = $self->_valspec()->{$table}{$col} || $self->_valspec()->{$ref_table}{$ref_col} ) {
        debugf("Value is specified. : " . Dumper($valspec_col));

        # 
        #  (1)値の決定方法に指定がある場合は、その方法により値を決定する。
        #
        $value = $self->determine_value( $valspec_col );

        #  その値を持つレコードが参照先テーブルになければ、参照先にその値を持つレコードを新たに作成
        #  
        #  参照先テーブルの内容を毎回問い合わせるのは効率が悪いと考えたが、
        #  参照先のレコード数が大量の場合はメモリを浪費してしまうことも考慮し、
        #  あえて毎回問い合わせることにした。
        $self->_add_record_if_not_exist($ref_table, $ref_col, $value);

    }
    elsif ( my $column_default = $self->_table_def($table)->column_def($col)->column_default ) {
        debugf("Column default is specified. value = $column_default");
        $value = $column_default;
        $self->_add_record_if_not_exist($ref_table, $ref_col, $value);

    }
    else {
        debugf("No value is specified. Trying to retrieve list of ids from $ref_table");

        #
        #  (2)値の決定方法にユーザ指定がない場合
        #

        #  現在参照先テーブルにあるPK値を取得する
        #  結果は 
        #  $ref_ids => { (id1)  => 1, (id2) => 1, ... }  という形で取得される。
        my $ref_ids = $self->_get_current_distinct_values($ref_table, $ref_col); 
    

        #  現存する参照先の値から1つ適当に選ぶ(1レコード以上ある場合)
        my @_ref_ids = keys %$ref_ids;
        if ( @_ref_ids ) {
            $value = $_ref_ids[ int(rand() * scalar(@_ref_ids)) ];
            debugf("Referenced record id = $value");

        }
        else {
            #  参照先にはまだレコードがないので、適当に作成   
            $value = $self->process_table($ref_table);      #  IDを指定していないので適当な値がIDになるはず
            $self->_distinct_val()->{$ref_table}{$ref_col}{$value} = 1;            #  このIDを追加しておく
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
        if (    $self->_valspec()->{$table} 
                and $self->_valspec()->{$table}{$col} 
                and $real_id = $self->determine_value( $self->_valspec()->{$table}{$col} ) 
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
        unless ( defined( $self->_valspec()->{$table}{$col} ) ) {

            my $col_def = $table_def->column_def($col);

            #  auto_increment 列は指定の必要なし
            next if $col_def->is_auto_increment;

            #  default 値が指定されている場合はそれを使用するので、指定の必要なし
            #  XXX: default値あり、かつfk制約がある場合に上手く動かないので、
            #       default値がある場合であっても明示的に指定するようにする。
            #next if defined($col_def->column_default);

            #  NULL 値が認められているのであれば指定しない
            next if $col_def->is_nullable eq 'YES';

        }

        push @cols, $col;
    }

    return wantarray ? @cols : [ @cols ];
}


sub _table_def {
    my ($self, $table) = @_;

    $self->{_table_def}{$table} 
        ||= Test::HandyData::mysql::TableDef->new( dbh => $self->dbh, table_name => $table );

    return $self->{_table_def}{$table};
}



#  _val_varchar($col_def, $exp_id)
#
#  Creates a new varchar value.
#  
#  $col_def : ColumnDef object.
#  $exp_id  : an expected value of primary key.
#
sub _val_varchar {
    my ($self, $col_def, $exp_id) = @_;

    my $maxlen = $col_def->character_maximum_length;
    debugf("Maxlen is $maxlen");

    if ( defined $exp_id ) {
        my $pk_length = length($exp_id);
        my $colname = $col_def->name;
        my $colname_length = length($colname);

        if ( $colname_length + $pk_length + 1 <= $maxlen ) {       #  (colname)_(num)
            return sprintf("%s_%d", $colname, $exp_id);
        }
        elsif ( $pk_length + 1 <= $maxlen ) {                      #  (part_of_colname)_(num)
            my $part_of_colname = substr($colname, 0, $maxlen - $pk_length - 1);
            return sprintf("%s_%d", $part_of_colname, $exp_id);
        }
        elsif ( $pk_length == $maxlen ) {
            return $exp_id;
        }   
    }

    $maxlen > $LENGTH_LIMIT_VARCHAR 
        and $maxlen = $LENGTH_LIMIT_VARCHAR;
    debugf("Maxlen is $maxlen");

    my $string = '';
    for (1 .. $maxlen) {
        $string .= $VARCHAR_LIST[ int( rand() * $COUNT_VARCHAR_LIST ) ];
    }
    debugf("Result string is $string");

    return $string;

}


sub _val_tinyint {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_TINYINT_UNSIGNED) : int(rand() * $MAX_TINYINT_SIGNED);
}


sub _val_smallint {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_SMALLINT_UNSIGNED) : int(rand() * $MAX_SMALLINT_SIGNED);
}

sub _val_int {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return (($type || '') =~ /unsigned/) ? int(rand() * $MAX_INT_UNSIGNED) : int(rand() * $MAX_INT_SIGNED);
}


sub _make_float {
    my ($precision, $scale) = @_;
    
    my $num = '';
    $num .= int(rand() * 10) for 1 .. $precision - $scale;
    if ( $num =~ /^0+$/ ) {
        $num = '0'
    }
    else {
        $num =~ s/^0+//; 
    }

    if ( $scale > 0 ) {
        $num .= '.';
        my $frac = '';
        $frac .= int(rand() * 10) for 1 .. $scale;
        if ( $frac =~ /^0+$/ ) {
            $frac = '0';
        }
        else {
            $frac =~ s/0+$//;
        }

        $num .= $frac;
    }

    return $num;
}


sub _val_numeric {
    my ($self, $col_def) = @_;

    my $precision = $col_def->numeric_precision;
    my $scale     = $col_def->numeric_scale;

    return _make_float($precision, $scale);
}


sub _val_float {
    my ($self, $col_def) = @_;

    my $type = $col_def->column_type;

    return _make_float($FLOAT_PRECISION, $FLOAT_SCALE);
}



sub _val_datetime {
    my ($self, $col_def) = @_;

    my $dt = DateTime->from_epoch( epoch => time + rand() * $RANGE_YEAR_DATETIME * $ONE_YEAR_SEC - $ONE_YEAR_SEC );

    if ($col_def->data_type eq 'date') {
        return $dt->ymd('-');
    }
    else {
        return $dt->ymd('-') . ' ' . $dt->hms(':');
    }
}


sub _val_year {
    my $dt = DateTime->from_epoch( epoch => time + rand() * $RANGE_YEAR_YEAR * $ONE_YEAR_SEC - $ONE_YEAR_SEC );

    return $dt->year();
}


=cut _get_current_distinct_values($table, $col)

$table, $col で指定された表・列の値(distinct値)を一定個数取得する。


=cut

sub _get_current_distinct_values {
    my ($self, $table, $col) = @_;

    my $current;

    #  At first, I tried to cache distinct values, but when user delete records, 
    #  those cached values are incorrect, and Test::Handy data has no idea
    #  which records have been already deleted.
    #  So I decide not to cache distinct values and query them every time. 

    #my $current = $self->_distinct_val()->{$table}{$col};
    #if ( !defined $current or keys %$current == 0 ) {

        #  現存するレコードを確認
        #  SELECT DISTINCT $col FROM $table LIMIT $DISTINCT_VAL_FETCH_LIMIT;
        my $select = $self->_sql_maker->new_select(distinct => 1);
        my ($sql, @bind) = $select->add_select($col)
                            ->add_from($table)
                            ->limit($DISTINCT_VAL_FETCH_LIMIT)
                            ->as_sql();

        my $res = $self->dbh()->selectall_arrayref($sql, undef, @bind);

        my %values = map { $_->[0] => 1 } @$res;

        $current = $self->_distinct_val()->{$table}{$col} = { %values };
    #}

    return $current;
}


=cut _set_user_valspec($table_name, $valspec)

insert を実行するときの条件を設定する。
(これまで設定していた条件はクリアされる)


=cut

sub _set_user_valspec {
    my ($self, $table, $table_valspec) = @_;

    #  前回の条件をクリア
    $self->_valspec({});

    $self->_add_user_valspec($table, $table_valspec);
}


=cut _add_user_valspec($table, $table_valspec)

次回 insert を実行したときの条件を設定する。
(これまで設定していた条件に追加する)

=cut

sub _add_user_valspec {
    my ($self, $table, $table_valspec) = @_;

    defined $table and length($table) > 0
        or confess "Missing table name";

    defined $table_valspec and ref $table_valspec eq 'HASH'
        or confess "Invalid user valspec. " . Dumper($table_valspec);


    for my $col (keys %$table_valspec) {
         
        my $_table = $table;
        my $_col   = $col;

        if ( $col =~ /\./ ) {
            ($_table, $_col, my @_dummy) = split '\.', $col;

            #  column name may include only one dot.
            defined($_table) and length($_table) > 0 
            and defined($_col) and length($_col) > 0
            and @_dummy == 0 
                or confess "Invalid column name : $col"; 
        }

        my $val = $table_valspec->{$col};

        #  At first, clear all values with the same key.
        delete $self->_valspec()->{$_table}{$_col};

        if ( ref $val eq 'ARRAY' ) {
            #  arrayref : select one from the list randomly.
            $self->_valspec()->{$_table}{$_col}{random} = $val;

        }
        elsif ( ref $val eq 'HASH' ) {
            #  hash : 
            #  currently { random => [ ... ] } or { fixval => $scalar } 
            #  may be specified.
            for (keys %$val) {
                $self->_valspec()->{$_table}{$_col}{$_} = $val->{$_};
            }

        }
        elsif ( ref $val eq '' ) {
            #  scalar : fix value
            $self->_valspec()->{$_table}{$_col}{fixval} = $val;

        }
        else {
            confess "NOTREACHED";
        }

    }

    debugf("Valspec is " . Dumper($self->_valspec()));

}   


=head2 inserted()

Returns all primary keys of inserted records by this instance. Return value is a hashref like this:

    my $ret = $hd->inserted();
    
    #  $ret = {
    #    'table_name1' => [ 10, 11 ],
    #    'table_name2' => [ 100, 110, 120 ],
    #  };

CAUTION: inserted() ignores records with no primary key, or primary key with multiple columns.

=cut



=head2 delete_all()

deletes all rows inserted by this instance.

CAUTION: delete_all() won't delete rows in tables which don't have primary key, or which have primary key with multiple columns.

=cut

sub delete_all {
    my ($self) = @_;

    my $dbh = $self->dbh();

    my $fk_check = $self->_check_fk_check_status();

    if ( $fk_check eq 'ON' or $fk_check == 1 ) {
        $dbh->do('SET FOREIGN_KEY_CHECKS = 0');
    }

    for my $table ( keys %{ $self->inserted() } ) {
        my $pk_name = $self->_table_def($table)->pk_columns()->[0];

        for my $val ( @{ $self->inserted->{$table} } ) {
            my ($sql, @bind) = $self->_sql_maker->delete($table, { $pk_name => $val });
            $dbh->do($sql, undef, @bind);
            debugf(qq{DELETE FROM `$table` WHERE `$pk_name` = "$val"});
        }
    }

    if ( $fk_check eq 'ON' or $fk_check == 1 ) {
        $dbh->do('SET FOREIGN_KEY_CHECKS = 1');
    }
}


sub _check_fk_check_status {
    my ($self) = @_;

    my @rows = $self->dbh->selectrow_array(q{SHOW VARIABLES LIKE '%foreign_key_checks%'});

    return $rows[1];
}


=cut _add_record_if_not_exist($table, $col, $value)

Inserts a record only if record(s) which value of column $col is $value doesn't exist.

=cut

sub _add_record_if_not_exist {
    my ($self, $table, $col, $value) = @_;

    if ( 0 == $self->_value_exists_in_table_col($table, $col, $value) ) {     #  No record exists
        $self->process_table($table, { $col => $value });       #  レコード作成
        debugf("A referenced record created. id = $value");
    }
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
Please report problems to <Maintainer name(s)>  (<contact address>)
Patches are welcome.

=head1 AUTHOR

Egawata C<< <egawa.takashi@gmail.com> >>


=head1 LICENCE AND COPYRIGHT

Copyright (c)2012-2013 Egawata C<< <egawa.takashi@gmail.com> >>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

