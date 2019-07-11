/*Top ten origins with the highest AVG departure delay
*/

REGISTER '/usr/local/pig/contrib/piggybank/java/piggybank.jar'

A = load '/Pigprac/input/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

B = foreach A generate (int)$16 as dep_delay, (chararray)$17 as origin;

C = filter B by (dep_delay is not null) AND (origin is not null);

D = group C by origin;

E = foreach D generate group, AVG(C.dep_delay);

Result = order E by $1 DESC;

Top_ten = limit Result 10;

A1 = load '/Pigprac/input/airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

A2 = foreach A1 generate (chararray)$0 as origin, (chararray)$2 as city, (chararray)$4 as country;

Joined = join A2 by origin, Top_ten by $0;

Final = foreach Joined generate $0,$1,$2,$4;

Final_Result = ORDER Final by $3 DESC;

dump Final_Result;
