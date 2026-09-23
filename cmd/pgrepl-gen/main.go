// pgrepl-gen is the schema discovery and Go code generation command for cdc.
package main

import (
	"context"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type config struct {
	Version        string        `yaml:"version"`
	DatabaseURL    string        `yaml:"database_url,omitempty"`
	DatabaseURLEnv string        `yaml:"database_url_env"`
	Publication    string        `yaml:"publication"`
	Slot           string        `yaml:"slot"`
	Tables         []tableConfig `yaml:"tables"`
}

type tableConfig struct {
	Schema  string `yaml:"schema"`
	Name    string `yaml:"name"`
	Enabled bool   `yaml:"enabled"`
}

type column struct {
	Name     string
	DataType string
	UDTName  string
	Nullable bool
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "version":
		fmt.Println("pgrepl-gen " + currentVersion())
	case "init":
		err = initCommand(os.Args[2:])
	case "generate":
		err = generateCommand(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgrepl-gen:", err)
		os.Exit(1)
	}
}

const version = "v0.1.1"

func currentVersion() string {
	info, ok := debug.ReadBuildInfo()
	if ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pgrepl-gen <init|generate|version> [flags]")
}

func initCommand(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	dsn := flags.String("dsn", os.Getenv("CDC_DATABASE_URL"), "PostgreSQL connection string (defaults to CDC_DATABASE_URL)")
	configPath := flags.String("config", "pgrepl.yaml", "configuration file to create")
	refresh := flags.Bool("refresh", false, "refresh an existing table list while preserving enabled flags")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var previous *config
	if *refresh {
		if data, err := os.ReadFile(*configPath); err == nil {
			var old config
			if err := yaml.Unmarshal(data, &old); err != nil {
				return fmt.Errorf("read existing config: %w", err)
			}
			previous = &old
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if *dsn == "" && previous != nil {
		*dsn = previous.DatabaseURL
		if *dsn == "" {
			envName := previous.DatabaseURLEnv
			if envName == "" {
				envName = "CDC_DATABASE_URL"
			}
			*dsn = os.Getenv(envName)
		}
	}
	if *dsn == "" {
		return fmt.Errorf("provide --dsn, set CDC_DATABASE_URL, or refresh a config containing database_url")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name`)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	var tables []tableConfig
	enabled := map[string]bool{}
	if previous != nil {
		for _, table := range previous.Tables {
			enabled[table.Schema+"\x00"+table.Name] = table.Enabled
		}
	}
	for rows.Next() {
		var table tableConfig
		if err := rows.Scan(&table.Schema, &table.Name); err != nil {
			rows.Close()
			return err
		}
		table.Enabled = true
		if oldEnabled, ok := enabled[table.Schema+"\x00"+table.Name]; ok {
			table.Enabled = oldEnabled
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(tables) == 0 {
		return fmt.Errorf("no user tables found")
	}
	cfg := config{Version: "1", DatabaseURL: *dsn, Publication: "app_cdc_pub", Slot: "app_cdc_slot", Tables: tables}
	if previous != nil {
		if previous.Publication != "" {
			cfg.Publication = previous.Publication
		}
		if previous.Slot != "" {
			cfg.Slot = previous.Slot
		}
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	if _, err := os.Stat(*configPath); err == nil && !*refresh {
		return fmt.Errorf("%s already exists; remove it or choose another --config path", *configPath)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.WriteFile(*configPath, data, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(*configPath, 0o600); err != nil {
		return err
	}
	verb := "Wrote"
	if previous != nil {
		verb = "Updated"
	}
	fmt.Printf("%s %s with %d tables. Set enabled: false for tables to ignore.\n", verb, *configPath, len(tables))
	return nil
}

func generateCommand(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	configPath := flags.String("config", "pgrepl.yaml", "configuration file")
	outputDir := flags.String("out", "internal/cdc", "generated package directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.Version != "1" {
		return fmt.Errorf("unsupported config version %q", cfg.Version)
	}
	if cfg.Publication == "" || cfg.Slot == "" {
		return fmt.Errorf("publication and slot must be set in %s", *configPath)
	}
	dsn := cfg.DatabaseURL
	if dsn == "" {
		envName := cfg.DatabaseURLEnv
		if envName == "" {
			envName = "CDC_DATABASE_URL"
		}
		dsn = os.Getenv(envName)
	}
	if dsn == "" {
		return fmt.Errorf("database_url is missing from %s; rerun init with --dsn", *configPath)
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	var tables []generatedTable
	for _, table := range cfg.Tables {
		if !table.Enabled {
			continue
		}
		columns, err := loadColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(columns) == 0 {
			return fmt.Errorf("enabled table %s.%s has no columns or does not exist", table.Schema, table.Name)
		}
		tables = append(tables, generatedTable{tableConfig: table, Type: exportedName(table.Name) + "Row", Columns: columns})
	}
	if len(tables) == 0 {
		return fmt.Errorf("configuration has no enabled tables")
	}
	if err := uniqueNames(tables); err != nil {
		return err
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema == tables[j].Schema {
			return tables[i].Name < tables[j].Name
		}
		return tables[i].Schema < tables[j].Schema
	})
	source, err := render(tables, cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(*outputDir, "cdc.gen.go")
	if err := os.WriteFile(path, source, 0o644); err != nil {
		return err
	}
	fmt.Printf("Generated %s for %d tables.\n", path, len(tables))
	return nil
}

func loadColumns(ctx context.Context, conn *pgx.Conn, table tableConfig) ([]column, error) {
	rows, err := conn.Query(ctx, `SELECT column_name, data_type, udt_name, is_nullable = 'YES' FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`, table.Schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("read %s.%s columns: %w", table.Schema, table.Name, err)
	}
	defer rows.Close()
	var result []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.Name, &c.DataType, &c.UDTName, &c.Nullable); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

type generatedTable struct {
	tableConfig
	Type    string
	Columns []column
}

func uniqueNames(tables []generatedTable) error {
	seen := map[string]string{}
	baseCounts := map[string]int{}
	for _, table := range tables {
		baseCounts[table.Type]++
	}
	for i := range tables {
		if baseCounts[tables[i].Type] > 1 {
			tables[i].Type = exportedName(tables[i].Schema) + tables[i].Type
		}
		if prior, ok := seen[tables[i].Type]; ok {
			return fmt.Errorf("tables %s and %s generate the same Go type name %s", prior, tables[i].Schema+"."+tables[i].Name, tables[i].Type)
		}
		seen[tables[i].Type] = tables[i].Schema + "." + tables[i].Name
		fields := map[string]string{}
		for _, c := range tables[i].Columns {
			name := exportedName(c.Name)
			if name == "CDCFieldsPresent" {
				return fmt.Errorf("column %q in %s.%s conflicts with generated CDCFieldsPresent metadata", c.Name, tables[i].Schema, tables[i].Name)
			}
			if prior, ok := fields[name]; ok {
				return fmt.Errorf("columns %q and %q in %s.%s generate the same Go field %s", prior, c.Name, tables[i].Schema, tables[i].Name, name)
			}
			fields[name] = c.Name
		}
	}
	return nil
}

func goType(c column) string {
	var result string
	switch c.DataType {
	case "smallint":
		result = "int16"
	case "integer":
		result = "int32"
	case "bigint":
		result = "int64"
	case "real":
		result = "float32"
	case "double precision":
		result = "float64"
	case "boolean":
		result = "bool"
	case "text", "character varying", "character", "varchar":
		result = "string"
	case "date", "timestamp without time zone", "timestamp with time zone":
		result = "time.Time"
	case "bytea":
		result = "[]byte"
	default:
		result = "any"
	}
	if c.Nullable && result != "any" && result != "[]byte" {
		return "*" + result
	}
	return result
}

func render(tables []generatedTable, cfg config) ([]byte, error) {
	var b strings.Builder
	b.WriteString("// Code generated by pgrepl-gen. DO NOT EDIT.\npackage cdc\n\n")
	b.WriteString("import (\n\t\"context\"\n\t\"fmt\"\n\t\"reflect\"\n\t\"time\"\n\n\tcorecdc \"github.com/orbitalbase/orbital-cdc\"\n)\n\nvar _ time.Time\n\n")
	b.WriteString("// RuntimeConfig returns the replication configuration recorded in pgrepl.yaml.\nfunc RuntimeConfig(connString string) corecdc.Config { return corecdc.Config{ConnString: connString, Publication: " + strconv.Quote(cfg.Publication) + ", Slot: " + strconv.Quote(cfg.Slot) + "} }\n\n")
	for _, table := range tables {
		b.WriteString("// " + table.Type + " is a generated row type.\n")
		b.WriteString("type " + table.Type + " struct {\n")
		for _, c := range table.Columns {
			tag := strconv.Quote("json:" + strconv.Quote(c.Name))
			b.WriteString("\t" + exportedName(c.Name) + " " + goType(c) + " " + tag + "\n")
		}
		b.WriteString("\tCDCFieldsPresent map[string]bool `json:\"-\"`\n")
		b.WriteString("}\n\n")
		b.WriteString("func decode" + table.Type + "(row corecdc.Row) (" + table.Type + ", error) {\n\tvar result " + table.Type + "\n\tresult.CDCFieldsPresent = make(map[string]bool, len(row))\n")
		for _, c := range table.Columns {
			name := strconv.Quote(c.Name)
			field := exportedName(c.Name)
			b.WriteString("\tif value, ok := row[" + name + "]; ok { if _, unchanged := value.(corecdc.UnchangedToast); !unchanged { decoded, err := assign[" + goType(c) + "](value); if err != nil { return result, fmt.Errorf(\"decode field %s: %w\", " + name + ", err) }; result." + field + " = decoded; result.CDCFieldsPresent[" + name + "] = true } }\n")
		}
		b.WriteString("\treturn result, nil\n}\n\n")
	}
	b.WriteString("// Handler receives typed CDC callbacks for the enabled tables.\ntype Handler interface {\n")
	for _, table := range tables {
		name := strings.TrimSuffix(table.Type, "Row")
		b.WriteString("\tOn" + name + "Insert(context.Context, " + name + ") error\n")
		b.WriteString("\tOn" + name + "Update(context.Context, " + name + ", " + name + ") error\n")
		b.WriteString("\tOn" + name + "Delete(context.Context, " + name + ") error\n")
		b.WriteString("\tOn" + name + "Truncate(context.Context) error\n")
	}
	b.WriteString("}\n\n// Dispatcher converts generic runtime events into generated typed callbacks.\ntype Dispatcher struct { Handler Handler }\n\nfunc (d Dispatcher) Handle(ctx context.Context, event corecdc.Event) error {\n\tif d.Handler == nil { return fmt.Errorf(\"generated CDC handler is nil\") }\n\tswitch {\n")
	for _, table := range tables {
		key := table.Schema + "." + table.Name
		callback := strings.TrimSuffix(table.Type, "Row")
		b.WriteString("\tcase event.Schema == " + strconv.Quote(table.Schema) + " && event.Table == " + strconv.Quote(table.Name) + ":\n\t\tswitch event.Operation {\n")
		b.WriteString("\t\tcase corecdc.Insert:\n\t\t\trow, err := decode" + table.Type + "(event.New); if err != nil { return err }; return d.Handler.On" + callback + "Insert(ctx, row)\n")
		b.WriteString("\t\tcase corecdc.Update:\n\t\t\toldRow, err := decode" + table.Type + "(event.Old); if err != nil { return err }; newRow, err := decode" + table.Type + "(event.New); if err != nil { return err }; return d.Handler.On" + callback + "Update(ctx, oldRow, newRow)\n")
		b.WriteString("\t\tcase corecdc.Delete:\n\t\t\trow, err := decode" + table.Type + "(event.Old); if err != nil { return err }; return d.Handler.On" + callback + "Delete(ctx, row)\n")
		b.WriteString("\t\tcase corecdc.Truncate:\n\t\t\treturn d.Handler.On" + callback + "Truncate(ctx)\n")
		b.WriteString("\t\tdefault: return fmt.Errorf(\"no generated callback for operation %q on %s\", event.Operation, " + strconv.Quote(key) + ")\n\t\t}\n")
	}
	b.WriteString("\tdefault: return nil // Tables disabled in pgrepl.yaml are ignored.\n\t}\n}\n\n")
	b.WriteString("func assign[T any](input any) (T, error) {\n\tvar zero T\n\tif input == nil { return zero, nil }\n\tvalue := reflect.ValueOf(input)\n\ttarget := reflect.TypeOf((*T)(nil)).Elem()\n\tif value.Type().AssignableTo(target) { return value.Interface().(T), nil }\n\tif target.Kind() == reflect.Pointer && value.Type().ConvertibleTo(target.Elem()) { result := reflect.New(target.Elem()); result.Elem().Set(value.Convert(target.Elem())); return result.Interface().(T), nil }\n\tif value.Type().ConvertibleTo(target) { return value.Convert(target).Interface().(T), nil }\n\treturn zero, fmt.Errorf(\"cannot assign %T to %s\", input, target)\n}\n")
	formatted, err := format.Source([]byte(b.String()))
	if err != nil {
		return nil, fmt.Errorf("format generated Go source: %w\n%s", err, b.String())
	}
	return formatted, nil
}

func exportedName(value string) string {
	parts := strings.FieldsFunc(value, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	var b strings.Builder
	for _, part := range parts {
		runes := []rune(part)
		if len(runes) > 0 {
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	if b.Len() == 0 {
		return "Column"
	}
	name := b.String()
	if unicode.IsDigit([]rune(name)[0]) {
		name = "Field" + name
	}
	return name
}
