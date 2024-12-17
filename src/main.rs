use std::{env, process::exit};

use clap::{App, Arg, ArgMatches, SubCommand};
use testcontainers::Container;
use testcontainers_modules::postgres::Postgres;

mod db_queries;
mod generate;
mod migrate;
mod models;
mod query_generate;
mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    let mut matcher = build_app();
    let matches = matcher.get_matches_mut();

    if let Some(matches) = &matches.subcommand_matches(GENERATE) {
        generate(&get_options(GENERATE, matches)).await?;
    } else if let Some(matches) = &matches.subcommand_matches(MIGRATE) {
        migrate(&get_options(MIGRATE, matches)).await?;
    } else {
        matcher.print_help().unwrap();
        exit(1);
    }

    Ok(())
}

#[derive(Debug)]
struct Options {
    is_docker: bool,
    models: String,
    migrations: Option<String>,
    context: Option<String>,
    database_url: String,
    schemas: Option<Vec<String>>,
    tables: Option<Vec<String>>,
    force: bool,
}

fn get_options(subcommand: &str, matches: &ArgMatches) -> Options {
    let models = matches
        .value_of(MODELS)
        .ok_or_else(|| env::var(ENV_SQLGEN_MODELS_FOLDER))
        .expect("Could not get output modles folder")
        .to_string();

    let context = matches.value_of(CONTEXT).map(String::from);

    let database_url = matches
        .value_of(DATABASE)
        .ok_or_else(|| env::var(ENV_DATABASE_URL))
        .expect("Must provide either a database uri or `docker`")
        .to_string();

    let is_docker = database_url == DOCKER;

    let mut migrations = matches.value_of(MIGRATIONS).map(String::from);

    if migrations.is_none() {
        if subcommand == MIGRATE {
            migrations = Some(DEFAULT_MIGRATIONS_FOLDER.to_string());
        } else if is_docker {
            panic!("Migrations folder is required for docker database");
        }
    }

    let force = matches.is_present(FORCE);

    let schemas: Option<Vec<String>> = matches
        .values_of(SCHEMA)
        .map(|schemas| schemas.map(String::from).collect())
        .or_else(|| {
            env::var(ENV_SQLGEN_SCHEMA)
                .ok()
                .map(|s| s.split(',').map(String::from).collect())
        });
    let tables: Option<Vec<String>> = matches
        .values_of(TABLE)
        .map(|tables| tables.map(String::from).collect())
        .or_else(|| {
            env::var(ENV_SQLGEN_TABLE)
                .ok()
                .map(|t| t.split(',').map(String::from).collect())
        });

    let options = Options {
        is_docker,
        models,
        migrations,
        context,
        database_url,
        schemas,
        tables,
        force,
    };

    println!("OPTIONS: {:?}", options);

    options
}

fn launch_docker_container() -> (String, Option<Container<'static, Postgres>>) {
    let cli = Box::leak(Box::new(testcontainers::clients::Cli::default()));
    let container = cli.run(Postgres::default());
    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        container.get_host_port_ipv4(5432)
    );
    (database_url, Some(container))
}

async fn generate(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let (database_url, _container) = match options.is_docker {
        true => launch_docker_container(),
        false => (options.database_url.to_string(), None),
    };

    if options.is_docker || options.migrations.is_some() {
        run_sqlx_migrator(options.migrations.as_deref().unwrap(), &database_url).await;
    }

    generate::generate(
        &options.models,
        &database_url,
        options.context.as_deref(),
        options.force,
        options
            .tables
            .as_ref()
            .map(|tables| tables.iter().map(String::as_str).collect()),
        options
            .schemas
            .as_ref()
            .map(|schemas| schemas.iter().map(String::as_str).collect()),
    )
    .await
}

async fn get_pool(database_url: &str) -> sqlx::Pool<sqlx::Postgres> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Failed to connect to database pool at: {}: {}",
                database_url, error,
            )
        })
}

async fn run_sqlx_migrator(input_migrations_folder: &str, database_url: &str) {
    println!("Applying migrations from {}", input_migrations_folder);
    let pool = get_pool(database_url).await;

    let migrations_path = std::path::Path::new(input_migrations_folder);
    let migrator = sqlx::migrate::Migrator::new(migrations_path)
        .await
        .expect("Could not create migrations folder");
    migrator.run(&pool).await.expect("could not run migration");

    println!("Migrations applied!");
}

async fn migrate(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let (database_url, _container) = match options.is_docker {
        true => launch_docker_container(),
        false => (options.database_url.to_string(), None),
    };

    println!("Finding new migration differences");
    migrate::migrate(
        &options.models,
        options.migrations.as_deref().unwrap(),
        &database_url,
        options
            .tables
            .as_ref()
            .map(|tables| tables.iter().map(String::as_str).collect()),
        options
            .schemas
            .as_ref()
            .map(|schemas| schemas.iter().map(String::as_str).collect()),
    )
    .await
}

const GENERATE: &str = "generate";
const MIGRATE: &str = "migrate";
const MODELS: &str = "models";
const CONTEXT: &str = "context";
const DATABASE: &str = "database";
const DOCKER: &str = "docker";
const MIGRATIONS: &str = "migrations";
const SCHEMA: &str = "schema";
const TABLE: &str = "table";
const FORCE: &str = "force";
const DEFAULT_MODELS_FOLDER: &str = "src/models/";
const DEFAULT_MIGRATIONS_FOLDER: &str = "migrations/";
const ENV_SQLGEN_MODELS_FOLDER: &str = "SQLGEN_MODELS_FOLDER";
const ENV_SQLGEN_MIGRATIONS_FOLDER: &str = "SQLGEN_MIGRATIONS_FOLDER";
const ENV_SQLGEN_TABLE: &str = "SQLGEN_TABLE";
const ENV_SQLGEN_SCHEMA: &str = "SQLGEN_SCHEMA";
const ENV_DATABASE_URL: &str = "DATABASE_URL";
const ENV_SQLGEN_CONTEXT_NAME: &str = "SQLGEN_CONTEXT_NAME";
const ENV_SQLGEN_OVERWRITE: &str = "SQLGEN_OVERWRITE";

use lazy_static::lazy_static;

lazy_static! {
    static ref DATABASE_ARG: Arg<'static> = Arg::with_name(DATABASE)
        .short('d')
        .long(DATABASE)
        .value_name(ENV_DATABASE_URL)
        .takes_value(true)
        .help("Sets the database connection URL, or `docker` to spin up a testcontainer",);
    static ref MODELS_ARG: Arg<'static> = Arg::with_name(MODELS)
        .short('o')
        .long(MODELS)
        .default_value(DEFAULT_MODELS_FOLDER)
        .value_name(ENV_SQLGEN_MODELS_FOLDER)
        .takes_value(true);
    static ref MIGRATIONS_ARG: Arg<'static> = Arg::with_name(MIGRATIONS)
        .short('m')
        .long(MIGRATIONS)
        .value_name(ENV_SQLGEN_MIGRATIONS_FOLDER)
        .takes_value(true);
    static ref CONTEXT_ARG: Arg<'static> = Arg::with_name(CONTEXT)
        .short('c')
        .long(CONTEXT)
        .value_name(ENV_SQLGEN_CONTEXT_NAME)
        .help("The name of the context for calling functions. Defaults to DB name")
        .takes_value(true);
    static ref TABLE_ARG: Arg<'static> = Arg::with_name(TABLE)
        .short('t')
        .long(TABLE)
        .value_name(ENV_SQLGEN_TABLE)
        .takes_value(true)
        .use_delimiter(true)
        .multiple(true)
        .help("Specify the table name(s)");
    static ref SCHEMA_ARG: Arg<'static> = Arg::with_name(SCHEMA)
        .short('s')
        .long(SCHEMA)
        .takes_value(true)
        .use_delimiter(true)
        .multiple(true)
        .help("Specify the schema name(s)");
    static ref FORCE_ARG: Arg<'static> = Arg::new(FORCE)
        .short('f')
        .long(FORCE)
        .value_name(ENV_SQLGEN_OVERWRITE)
        .takes_value(false)
        .required(false)
        .help("Overwrites existing files sharing names in that folder");
}

fn build_app() -> App<'static> {
    let generate_subcommand = SubCommand::with_name(GENERATE)
        .about("Generate structs and queries for tables")
        .args([
            DATABASE_ARG.clone(),
            MODELS_ARG.clone()
                .help("Sets the output folder for generated structs"),
            MIGRATIONS_ARG.clone().help("The folder of migrations to apply. Leave blank if you do not wish to apply migrations before generating."),
            CONTEXT_ARG.clone(),
            SCHEMA_ARG.clone(),
            TABLE_ARG.clone(),
            FORCE_ARG.clone(),
        ]);

    let migrate_subcommand = SubCommand::with_name(MIGRATE)
        .about("Generate SQL migrations based on struct differences")
        .args([
            DATABASE_ARG.clone(),
            MODELS_ARG
                .clone()
                .help("Sets the folder containing existing struct files"),
            MIGRATIONS_ARG
                .clone()
                .default_value(DEFAULT_MIGRATIONS_FOLDER)
                .help("Sets the output folder for migrations"),
            SCHEMA_ARG.clone(),
            TABLE_ARG.clone(),
        ]);

    App::new("SQL Gen")
        .subcommand(generate_subcommand)
        .subcommand(migrate_subcommand)
}
