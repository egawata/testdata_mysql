package Test::HandyData::mysql::UniqGen::Char;

use strict;
use warnings;
#use parent qw(Test::HandyData::mysql::UniqGen);

use constant VALUE_MAX_LENGTH => 10;


sub new {
    my ($inv, %params) = @_;

    my $class = ref $inv || $inv;
    my $self = bless { %params }, $class;

    return $self;
}


sub dbh {
    my ($self, $dbh) = @_;

    $dbh
        and $self->{dbh} = $dbh;

    return $self->{dbh};
}


sub table {
    my ($self, $table) = @_;

    defined $table
        and $self->{table} = $table;

    return $self->{table};
}


sub column {
    my ($self, $column) = @_;

    defined $column 
        and $self->{column} = $column;

    return $self->{column};
}


sub generate {
    my ($self) = @_;


    my $size = $self->size;
    $size > VALUE_MAX_LENGTH and $size = VALUE_MAX_LENGTH;

    my ($prefix, $index);
    while ( !defined($prefix) or length($prefix) <= $size ) {
        $prefix = $self->current_prefix();
        if ( !defined($prefix) ) {

            #  初期値は2文字から始める。
            #  ただし列の最大長が1文字のときは1文字から始める。
            $prefix = ($size == 1) ? '0' : '00';

            $index = $self->get_next_index( $size - length($prefix) );
        }
        else {
            $index = $self->get_next_index( $size - length($prefix) );

            if ( !defined($index) ) {   #  現在のprefix で使用可能なものをすべて割り当ててしまった 

                #  次の prefix を探す
                $prefix = $self->search_next_available_prefix( length($prefix) );

                #  すでに使用可能な prefix が存在しない場合は、
                #  prefix 長を1文字伸ばしたものを探す。
                if ( !defined($prefix) and length($prefix) < $size ) {
                    $prefix = $self->search_next_available_prefix( length($prefix) + 1 );
                }

                $index = $self->get_next_index( length($prefix) );
            }
        }
    }
    
    confess "Failed to generate prefix" unless defined $prefix;

    my $value = $prefix . $index;
   
    return $value; 
}


sub current_prefix {
    my ($self) = @_;
    
    return $self->{prefix};
}


=pod search_next_available_prefix($length)

利用可能なプレフィクスを生成する。
ルールは以下の通り。

- プレフィクスは基本的に2文字で、0-9, A-Z, _ のみを使用する。ただし列のサイズが1文字の場合(varchar(1)など)は、プレフィクスは1文字となる。1文字の場合は以下のルールは適用しない。(別メソッドを使う)

- 文字列の大小比較は、小さい順に 0-9, A-Z, _ とする。(mysql の比較順と同じ)

- 現存レコードの列値の最初の2文字にどのようなものがあるかを調べる。(2文字とも 0-9, A-Z, _ であるもののみ)
　mysql では collation によっては大文字小文字を区別しないことがあるので、すべて大文字に変換したものを取得する。

- もしレコードが1件も存在しなければ、プレフィクスとして '00' を使う。

- もしレコードが1件以上存在する場合は、その中での最大値の次の値をプレフィクスとする。(例： 最大値が'TD'なら、その次は'TE')

- もし最大値が '__'(次がない)の場合は、'__' から逆順に空きを探していき、見つかったものをプレフィクスとする。

=cut

sub search_next_available_prefix {
    my ($self, $length) = @_;
  
    #  prefix 文字長が変わり、キャッシュしていたリストが使用できなくなった場合はクリアする。 
    length($self->{prefix}) != $length
        and $self->{prefix_list} = undef;
   
    
    my $prefix;
    my @prefix_list = $self->{prefix_list}
                      || $self->get_current_prefix_from_db($length);


    if ( scalar(@prefix_list) == 0 ) {
        $prefix = '0' x $length;
        push @prefix_list, $prefix;
    }
    elsif ( $prefix_list[-1] ne '_' x $length ) {
        $prefix = $self->get_next_prefix( $prefix_list[-1] );
        push @prefix_list, $prefix;
    }
    else {
        for ( 0 .. scalar(@prefix_list) - 2 ) {
            my $next_candidate = $self->get_next_prefix( $prefix_list[$_] );
            if ( $next_candidate lt $prefix_list[$_ + 1] ) {
                $prefix = $next_candidate;
                @prefix_list = ( 
                    @prefix_list[ 0 .. $_ ], 
                    $prefix, 
                    @prefix_list[ $_ + 1 .. scalar(@prefix_list) - 1 ] 
                );
                last;
            }
        }
        debugf "Failed to find prefix for unique key " . $self->table() . "." . $self->column();
        return undef;
    }

    $self->{prefix_list} = [ @prefix_list ];
    return $prefix;
}



=pod get_current_prefix_from_db($length)

現在、対象列の最初 $length 文字にどのようなものがあるかをDBに問い合わせる。
(0-9, A-Z, _ のみで構成されるもののみ)


=cut

sub get_current_prefix_from_db {
    my ($self, $length) = @_;

    my $dbh = $self->dbh();
    my $table = $self->table();
    my $column = $self->column();


    #  対象列の、最初の2文字にどのようなものがあるか調べる。
    my $prefix_list = $dbh->selectall_arrayref(qq{
        SELECT distinct( substr( upper($column), 1, $length ) )      
        FROM $table
    });


    #  0-9, A-Z, _ の文字 $length 文字のみで構成されるもののみ抽出し、ソートする。
    $prefix_list = sort grep { $_ =~ /^[0-9A-Z_]{$length}$/ } @$prefix_list; 


    return wantarray ? @$prefix_list : $prefix_list;
}


=pod get_next_prefix($curr_prefix)

$curr_prefix の次のプレフィクスを返す。
例えば

    'TD' => 'TE'
    'S_' => 'T0'

次がない場合('__') はundef を返す。

=cut

sub get_next_prefix {
    my ($self, $curr_prefix) = @_;
    
    return $self->get_next_value($curr_prefix);
}


sub get_next_value {
    my ($self, $current) = @_;

    my $length = length($current);

    $current =~ /^[0-9A-Z_]{$length}$/
        or confess "Invalid value : [$current]";
        
    my @letters = split //, $current; 

    for ( 0 .. $length - 1 ) {
        $next = $self->get_next_letter( $letters[ -1 - $_ ] );
        $letters[ -1 - $_ ] = $next;
        if ( $next ne '0' ) {
            last;
        }
    }

    my $next_value = join '', @letters;
   
    #  次が '0' の連続になったときは、次の value はない。 
    return ( $next_value eq '0' x $length ) ? undef : $next_value;     
}


sub get_next_letter {
    my ($self, $curr) = @_;
   
    my $next =    ( $curr =~ /^[0-8A-Y]$/ ) ? chr( ord($curr) + 1 )
                : ( $curr eq '9' ) ? 'A'
                : ( $curr eq 'Z' ) ? '_'
                : ( $curr eq '_' ) ? '0'
                : confess 'NOTREACHED'
                ;
    return $next;
}


=pod get_next_index($length)

インデックス値を得る。
前回同じ長さのインデックスを取得していた場合は、その次のインデックス値を返す。(例: 'TD' -> 'TE')
前回と長さが異なる場合は、'0' x $length を返す。(次回は '00...1')
  

=cut

sub get_next_index {
    my ($self, $length) = @_;
   
    return '' if $length == 0;
    
    my $index = $self->{index};
    if ( !defined($index) or length($index) != $length ) {
        $index = '0' x $length;
    }
    else {
        #  注：次の値が存在しない場合は、undef が返る。
        #  よって、if を抜けたあとに $self->{index} には undef が入る。
        #  その直後に get_next_index をもう一回コールすれば、次回は '0' x $length が得られる。
        $index = $self->get_next_value($index);
    }

    $self->{index} = $index;
    return $index;
}


1;



