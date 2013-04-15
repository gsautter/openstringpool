The repository hosts the Open Node Network and Open String Pool infrastructure,
a generic web application for sharing data. It is developed by Guido Sautter
on behalf of The Open University and Karlsruhe Institute of Technology (KIT)
under the ViBRANT project (EU Grant FP7/2007-2013, Virtual Biodiversity 
Research and Access Network for Taxonomy).

Copyright (C) 2011-2013 ViBRANT (FP7/2007-2013, GA 261532), by G. Sautter

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program (LICENSE.txt); if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.



PURPOSE

The Open Node Network infrastructure is not intended to be used as is: it implements
a generic infrastructure replication protocol which is ready to use, but no specific
data management facilities; the latter are for sub classes to implement.

The Open String Pool infrastructure builds on top of the Open Node Network. It
implements management and replication facilities for generic string data, each record
consisting of a plain string and optionally an atomized version in XML. Open String
Pool is not intended to be used as is, either, as customization to specific types of
string data is up to sub classes, as is the selection of an appropriate XML schema
for the atomized versions of the hosted strings.   

This is the reason the Ant build script generates a ZIP rather than a WAR file.



SYSTEM REQUIREMENTS

Java Runtime Environment 1.5 or higher, Sun/Oracle JRE recommended

Apache Tomcat 5.5 or higher (other servlet containers should work as well, but have not been tested yet)

A database server, e.g. PostgreSQL (drivers included for version 8.2) or Microsoft SQL Server (drivers included)
Instead, you can also use Apache Derby embedded database (included)
(using Apache Derby is the default configuration, so you can test RefBank without setting up a database)