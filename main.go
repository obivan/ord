package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/mattn/go-oci8"
)

type dbObjectType int

type dbObjectInfo struct {
	db     *sql.DB
	schema string
	name   string
	typ    dbObjectType
}

const (
	illegal dbObjectType = iota
	table
	index
	packageBody
)

var dbObjectTypes = [...]string{
	table:       "TABLE",
	index:       "INDEX",
	packageBody: "PACKAGE_BODY",
}

func newDbObjectInfo(db *sql.DB, sch, obj string) (*dbObjectInfo, error) {
	var rawType string
	sch, obj = strings.ToUpper(sch), strings.ToUpper(obj)
	err := db.QueryRow(`
		select object_type
		  from dba_objects
		 where owner = :owner
		   and object_name = :object
		   and object_type not in ('TABLE PARTITION', 'SYNONYM', 'PACKAGE')`,
		sch, obj).Scan(&rawType)
	if err != nil {
		return nil, err
	}
	return &dbObjectInfo{
		db:     db,
		schema: sch,
		name:   obj,
		typ:    lookupType(rawType),
	}, nil
}

func (o dbObjectInfo) getDDL() (string, error) {
	var ddl string
	err := o.db.QueryRow(
		"select dbms_metadata.get_ddl(:typ, :tbl, :owner) from dual",
		o.typ.String(), o.name, o.schema).Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, nil
}

func (o dbObjectInfo) getDependentDDL(typ dbObjectType) (string, error) {
	var ddl string
	err := o.db.QueryRow(
		"select dbms_metadata.get_dependent_ddl(:typ, :tbl, :owner) from dual",
		typ.String(), o.name, o.schema).Scan(&ddl)
	if err != nil {
		return "", err
	}
	return ddl, nil
}

func (o dbObjectInfo) String() string {
	switch o.typ {
	case packageBody:
		ddl, err := o.getDDL()
		if err != nil {
			closeAndExit(o.db, 1, err)
		}
		return ddl
	case table:
		tableChan := make(chan string)
		indexChan := make(chan string)
		go func() {
			ddl, _ := o.getDDL()
			tableChan <- ddl
			close(tableChan)
		}()
		go func() {
			ddl, _ := o.getDependentDDL(index)
			indexChan <- ddl
			close(indexChan)
		}()
		return fmt.Sprintf("%s\n%s", <-tableChan, <-indexChan)
	}
	return ""
}

func (t dbObjectType) String() string {
	return dbObjectTypes[t]
}

func lookupType(raw string) dbObjectType {
	// "PACKAGE BODY" must be "PACKAGE_BODY" in dbms_metadata.get_ddl
	if raw == "PACKAGE BODY" {
		return packageBody
	}
	for i, v := range dbObjectTypes {
		if v == raw {
			return dbObjectType(i)
		}
	}
	return illegal
}

func closeAndExit(db io.Closer, code int, message ...interface{}) {
	for _, m := range message {
		fmt.Fprintln(os.Stderr, m)
	}
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "db.Close() error: %s\n", err)
	}
	os.Exit(code)
}

func main() {
	var flags struct {
		dsn, sch, obj string
	}
	flag.StringVar(&flags.dsn, "dsn", "apps/apps@ehqe", "DSN")
	flag.StringVar(&flags.sch, "sch", "XXT", "Object schema")
	flag.StringVar(&flags.obj, "obj", "", "Object name")
	flag.Parse()

	db, err := sql.Open("oci8", flags.dsn)
	defer closeAndExit(db, 0)
	if err != nil {
		closeAndExit(db, 1, err)
	}

	_, err = db.Exec(`
		begin
			dbms_metadata.set_transform_param(dbms_metadata.session_transform,
				'SQLTERMINATOR',
				true);
		end;`)
	if err != nil {
		closeAndExit(db, 1, err)
	}

	info, err := newDbObjectInfo(db, flags.sch, flags.obj)
	if err != nil {
		closeAndExit(db, 1, err)
	}

	fmt.Println(info)
}
