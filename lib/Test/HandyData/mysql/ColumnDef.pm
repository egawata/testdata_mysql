package Test::HandyData::mysql::ColumnDef;

use strict;
use warnings;

use Carp;
use Data::Dumper;


=head2 new($colname, %params)

Constructor.

%params is a hash which contains a column definition retrieved from information_schema.columns. 


=cut

sub new {
    my ($inv, $colname, @defs) = @_;

    my %params = ();
    if (@defs == 1 and ref $defs[0] eq 'HASH') {
        %params = %{ $defs[0] };
    }
    elsif (@defs % 2 == 0) {
        %params = @defs;
    }
    else {
        confess "Invalid nums of defs. num = " . scalar(@defs);
    }

    for my $key (keys %params) {
        if ( uc $key ne $key ) {
            $params{uc $key} = delete $params{$key};
        }
    }

    my $class = ref $inv || $inv;
    my $self = bless { name => $colname, %params }, $class;

    return $self;
}


sub is_auto_increment {
    my ($self) = @_;

    return ( $self->{EXTRA} =~ /auto_increment/ ) ? 1 : 0;
}


sub AUTOLOAD {
    my ($self) = @_;

    our $AUTOLOAD;
    $AUTOLOAD =~ /::(\w+)$/;
    my $key = uc($1);

    return if $key eq 'DESTROY';   #  do nothing

    if ( exists($self->{$key}) ) {
        return $self->{$key};
    }
    else {
        confess "[$AUTOLOAD] : no such attribute";
    }

}


1;

