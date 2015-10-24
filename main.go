package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-oci8"
)

var (
	dsn = flag.String("dsn", "apps/apps@ehqe", "DSN")
	sch = flag.String("sch", "XXT", "Object schema")
	obj = flag.String("obj", "", "Object name")
)

func main() {
	flag.Parse()

	*sch = strings.ToUpper(*sch)
	*obj = strings.ToUpper(*obj)

	db, err := sql.Open("oci8", *dsn)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}

	// if err := db.Ping(); err != nil {
	// 	log.Fatal(err)
	// }

	// log.Println("Connected")

	_, err = db.Exec(`
begin
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,
                                    'SQLTERMINATOR',
                                    true);
end;`)
	if err != nil {
		log.Fatal(err)
	}

	t, err := typeOf(db, *obj)
	if err != nil {
		log.Fatal(err)
	}
	// log.Printf("Type of %s => %s\n", *obj, t)
	switch t {
	case "TABLE":
		tableChan := make(chan string)
		indexChan := make(chan string)
		go getTableInfo(db, tableChan)
		go getIndexInfo(db, indexChan)
		fmt.Println(strings.Replace(<-tableChan, `"`, ``, -1))
		fmt.Println(strings.Replace(<-indexChan, `"`, ``, -1))
	case "PACKAGE BODY":
		packageChan := make(chan string)
		go getPackageBody(db, packageChan)
		fmt.Println(<-packageChan)
	}
	// log.Println("Done")
}

func typeOf(db *sql.DB, name string) (objectType string, err error) {
	err = db.QueryRow(`
select object_type
  from dba_objects
 where owner = :owner
   and object_name = :object
   and object_type not in ('TABLE PARTITION', 'SYNONYM', 'PACKAGE')`,
		*sch, *obj).Scan(&objectType)
	if err != nil {
		return "", err
	}
	return objectType, err
}

func getTableInfo(db *sql.DB, result chan string) {
	var ddl string
	err := db.QueryRow(
		`select dbms_metadata.get_ddl('TABLE', :tbl, :owner) from dual`,
		*obj, *sch).Scan(&ddl)
	if err != nil {
		log.Fatalf("Can't get table info: %s", err)
	}
	result <- ddl
	close(result)
}

func getIndexInfo(db *sql.DB, result chan string) {
	var ddl string
	err := db.QueryRow(
		`select dbms_metadata.get_dependent_ddl('INDEX', :tbl, :owner) from dual`,
		*obj, *sch).Scan(&ddl)
	if err != nil {
		log.Fatalf("Can't get table index info: %s", err)
	}
	result <- ddl
	close(result)
}

func getPackageBody(db *sql.DB, result chan string) {
	var ddl string
	err := db.QueryRow(
		`select dbms_metadata.get_ddl('PACKAGE_BODY', :tbl, :owner) from dual`,
		*obj, *sch).Scan(&ddl)
	if err != nil {
		log.Fatalf("Can't get package body: %s", err)
	}
	result <- ddl
	close(result)
}
