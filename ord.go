package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"log"
	"strings"
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

	_, err = db.Exec("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', true); end;")
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
		tableDDL, err := getTableInfo(db)
		if err != nil {
			log.Fatal(err)
		}
		indexDDL, err := getIndexInfo(db)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(strings.Replace(tableDDL, "\"", "", -1))
		fmt.Println(strings.Replace(indexDDL, "\"", "", -1))
	case "PACKAGE BODY":
		packageDDL, err := getPackageBody(db)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(packageDDL)
	}
	// log.Println("Done")
}

func typeOf(db *sql.DB, name string) (objectType string, err error) {
	err = db.QueryRow(`select object_type
		                 from dba_objects
		                where owner = :owner
		                  and object_name = :object
		                  and object_type not in ('TABLE PARTITION', 'SYNONYM', 'PACKAGE')`, *sch, *obj).Scan(&objectType)
	if err != nil {
		return "", err
	}
	return objectType, err
}

func getTableInfo(db *sql.DB) (ddl string, err error) {
	err = db.QueryRow("select dbms_metadata.get_ddl('TABLE', :tbl, :owner) from dual", *obj, *sch).Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, err
}

func getIndexInfo(db *sql.DB) (ddl string, err error) {
	err = db.QueryRow("select dbms_metadata.get_dependent_ddl('INDEX', :tbl, :owner) from dual", *obj, *sch).Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, err
}

func getPackageBody(db *sql.DB) (ddl string, err error) {
	err = db.QueryRow("select dbms_metadata.get_ddl('PACKAGE_BODY', :tbl, :owner) from dual", *obj, *sch).Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, err
}
