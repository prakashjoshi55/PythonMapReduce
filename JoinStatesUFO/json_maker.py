<?php
$myFile = "states.csv";
$fh = fopen($myFile, 'r');
$fh_w = fopen("states.json", 'w');
while(! feof($fh)){
    $line = fgetcsv($fh);
    fwrite($fh_w, json_encode(array('name'=>$line[0],'state'=>$line[1],'inhb'=>$line[2]))."\n");
}
fclose($fh);
fclose($fh_w);
?>
