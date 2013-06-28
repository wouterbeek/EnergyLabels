This code creates RDF representations of energy label data that is provided in an XML format.

The code is written in SWI-Prolog and requires modules from the Prolog Generics Collection in order to work. See https://github.com/wouterbeek/PrologGenerics

The script assumes the input files are in a subdirectory called 'in' and places the output files in a subdirectory called 'out'. Other directories and input file names can be set in the top of the Prolog file ('SETTINGS' section).

The translation is as follows:
1. File v20130401.dx contains one line of XML. This is split into multiple lines (cutoff between end and start tags). Note: for this a byte splitting method is used. To circumvent within-character splits we take multiples of 2 bytes.
2. The resultant XML file with newlines is split in smaller XML files based on the postal code of the entries it contains. Currently we create an XML file for every first two characters of postal codes.
3. The small XML files' DOM can easily be read into memory. The mapping from DOM to RDF is relatively easy. There are some inaccuracies in the data that one should deal with (see Prolog file). This step results in a small Turtle file for each small XML file.
4. Then we assemble the small Turtle files into one big Turtle file.

The above process can be completely scripted. One can skip the generation of small RDF files in step 3, but these small files can come in handy during debugging.

Wouter Beek
me@wouterbeek.com
2013/04/08
