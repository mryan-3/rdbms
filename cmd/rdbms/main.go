package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mryan-3/rdbms/internal/repl"
	"github.com/mryan-3/rdbms/internal/storage"
)

func main() {
	version := flag.Bool("version", false, "Show version information")
	help := flag.Bool("help", false, "Show help information")
	sqlFile := flag.String("file", "", "Execute SQL from file")

	flag.Parse()

	if *version {
		fmt.Println("RDBMS v1.0.0")
		fmt.Println("A simple relational database management system")
		os.Exit(0)
	}

	if *help {
		fmt.Println("RDBMS - Simple Relational Database Management System")
		fmt.Println("\nUsage:")
		fmt.Println("  rdbms [options]")
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
		fmt.Println("\nCommands:")
		fmt.Println("  help     Show this help message")
		fmt.Println("  quit     Exit REPL")
		fmt.Println("  \\d       List all tables")
		fmt.Println("  \\d table Describe table schema")
		fmt.Println("  \\dt      List all tables")
		fmt.Println("  \\s       Show full database schema")
		fmt.Println("\nExamples:")
		fmt.Println("  rdbms")
		fmt.Println("  rdbms -file schema.sql")
		os.Exit(0)
	}

	db := storage.NewDatabase()

	r := repl.NewREPL(db)

	if *sqlFile != "" {
		if err := r.ImportFile(*sqlFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error importing SQL file: %v\n", err)
			os.Exit(1)
		}
	}

	if len(flag.Args()) > 0 && flag.Args()[0] == "help" {
		r.Run()
	} else {
		if err := r.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}
}
