package Test::HandyData::mysql::ColumnDef;

use strict;
use warnings;

use Carp;
use Data::Dumper;


=head1 NAME

Test::HandyData::mysql::ColumnDef - Manages one column definition 


=head1 VERSION

This documentation refers to Test::HandyData::mysql::ColumnDef version 0.0.1


=head1 SYNOPSIS

    use Test::HandyData::mysql::ColumnDef;
    
    my $cd = Test::HandyData::mysql::ColumnDef->new('colname', %column_definition);

    #  true if 'colname' is auto_increment
    my $res = $cd->is_auto_increment();
    
    #  get column type 
    my $type = $cd->data_type();
    

=head1 DESCRIPTION

This class is a container of column definition retrieved from information_schema.columns.


=head1 METHODS 


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


=head2 name()

Returns column name.

=cut

sub name { shift->{name}; }


=head2 is_auto_increment()

Returns 1 if the column is auto_increment. Otherwise returns 0.

=cut

sub is_auto_increment {
    my ($self) = @_;

    return ( $self->{EXTRA} =~ /auto_increment/ ) ? 1 : 0;
}


=head2 To retrieve other attributes

information_schema.columns has many attributes. You can retrieve one of them by using a method which name corresponds to attribute name in lowercase.

For example, you can retrieve 'DATA_TYPE' like this:

    $type = $column_def->data_type();

=cut

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

Egawata


=head1 LICENCE AND COPYRIGHT

Copyright (c)2013 Egawata All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
