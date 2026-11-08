//! What the command line accepts, and what it refuses before any request.

use clap::CommandFactory;
use clap::error::ErrorKind;

use super::*;

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";
const FLOW: &str = "019a5c2e7f00a1b2c3d4e5f63f2a9c01";

/// Parses `burgess <args>`. A server is given when `args` has none, thus
/// a `BURGESS_SERVER` in the environment of the test does not change it.
fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let server = (!args.contains(&"--server")).then_some(["--server", DEFAULT_SERVER]);
    let args = std::iter::once("burgess")
        .chain(server.into_iter().flatten())
        .chain(args.iter().copied());
    Cli::try_parse_from(args)
}

fn refused(args: &[&str]) -> clap::Error {
    parse(args).expect_err("the command line is refused")
}

#[test]
fn a_tree_takes_a_tree_id_or_a_flow_id() {
    for id in [TREE, FLOW] {
        let Command::Tree { id: parsed, .. } = parse(&["tree", id]).unwrap().command else {
            panic!("a tree lookup");
        };
        assert_eq!(parsed.tree().to_string(), TREE);
    }
}

#[test]
fn a_flow_takes_only_a_flow_id() {
    let Command::Flow { id, .. } = parse(&["flow", FLOW]).unwrap().command else {
        panic!("a flow lookup");
    };
    assert_eq!(id.to_string(), FLOW);

    let error = refused(&["flow", TREE]);
    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    assert!(
        error
            .to_string()
            .contains("expected 32 hex characters (a flow id), found 24"),
        "{error}"
    );
}

#[test]
fn an_id_that_is_not_hex_is_refused_with_the_parsers_words() {
    let error = refused(&["tree", "019a5c2e-7f00"]);
    assert!(
        error
            .to_string()
            .contains("invalid character '-' at position 8"),
        "{error}"
    );
}

#[test]
fn ids_are_read_in_either_case() {
    let Command::Tree { id, .. } = parse(&["tree", &TREE.to_uppercase()]).unwrap().command else {
        panic!("a tree lookup");
    };
    assert_eq!(id.to_string(), TREE);
}

#[test]
fn the_server_defaults_to_a_local_service() {
    // Read from the declaration rather than by parsing, since parsing reads
    // `BURGESS_SERVER` from the environment the tests run in.
    let command = Cli::command();
    let server = command
        .get_arguments()
        .find(|argument| argument.get_id() == "server")
        .unwrap();
    assert_eq!(server.get_default_values(), [DEFAULT_SERVER]);
    assert_eq!(
        server.get_env(),
        Some(std::ffi::OsStr::new("BURGESS_SERVER"))
    );
}

#[test]
fn defaults_ask_for_a_listing_with_the_services_own_options() {
    let cli = parse(&["tree", TREE]).unwrap();
    assert_eq!(cli.output, Output::Listing);
    let Command::Tree { page, .. } = cli.command else {
        panic!("a tree lookup");
    };
    assert_eq!(
        page,
        PageArgs {
            until: None,
            limit: None
        }
    );
}

#[test]
fn server_and_output_may_come_before_or_after_the_command() {
    for args in [
        [
            "--server",
            "http://burgess:9000",
            "-o",
            "jsonl",
            "tree",
            TREE,
        ],
        [
            "tree",
            TREE,
            "--server",
            "http://burgess:9000",
            "--output",
            "jsonl",
        ],
    ] {
        let cli = parse(&args).unwrap();
        assert_eq!(
            cli.server.uri().authority().unwrap().as_str(),
            "burgess:9000"
        );
        assert_eq!(cli.output, Output::JsonLines);
    }
}

#[test]
fn until_is_read_as_rfc_3339_or_unix_milliseconds() {
    let expected = Timestamp::from_millisecond(1_793_880_000_000).unwrap();
    for until in [
        "1793880000000",
        "2026-11-05T12:00:00Z",
        "2026-11-05T13:00:00+01:00",
    ] {
        let Command::Flow { page, .. } = parse(&["flow", FLOW, "--until", until]).unwrap().command
        else {
            panic!("a flow lookup");
        };
        assert_eq!(page.until, Some(expected), "{until}");
    }
}

#[test]
fn an_until_that_is_no_time_is_refused() {
    let error = refused(&["tree", TREE, "--until", "yesterday"]);
    assert!(
        error
            .to_string()
            .contains("\"yesterday\" is not an RFC 3339 time or unix milliseconds"),
        "{error}"
    );
}

#[test]
fn a_limit_is_a_positive_number() {
    let Command::Tree { page, .. } = parse(&["tree", TREE, "--limit", "20"]).unwrap().command
    else {
        panic!("a tree lookup");
    };
    assert_eq!(page.limit, NonZeroU32::new(20));
    for limit in ["0", "-1", "many", "4294967296"] {
        assert_eq!(
            refused(&["tree", TREE, &format!("--limit={limit}")]).kind(),
            ErrorKind::ValueValidation,
            "{limit}"
        );
    }
}

#[test]
fn a_server_that_grpc_cannot_use_is_refused() {
    let error = refused(&["--server", "http://burgess:9000/burgess", "tree", TREE]);

    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    assert!(error.to_string().contains("expected no path"), "{error}");
}

#[test]
fn an_unknown_output_is_refused() {
    assert_eq!(
        refused(&["-o", "yaml", "tree", TREE]).kind(),
        ErrorKind::InvalidValue
    );
}

#[test]
fn states_take_repeated_labels_in_their_order() {
    let Command::States { states, .. } = parse(&[
        "states",
        "checkout",
        "payment",
        "--meta",
        "region=eu",
        "--meta",
        "note=a=b",
        "--meta",
        "empty=",
    ])
    .unwrap()
    .command
    else {
        panic!("a states lookup");
    };
    assert_eq!(
        states.meta,
        [
            ("region".to_string(), "eu".to_string()),
            ("note".to_string(), "a=b".to_string()),
            ("empty".to_string(), String::new()),
        ]
    );
}

#[test]
fn a_label_without_a_key_and_a_value_is_refused() {
    for label in ["region", "=eu"] {
        let error = refused(&["states", "a", "b", "--meta", label]);
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{label}");
    }
}

#[test]
fn states_take_an_order_by_its_name() {
    for (name, order) in [("asc", TsOrder::Asc), ("desc", TsOrder::Desc)] {
        let Command::States { states, .. } = parse(&["states", "a", "b", "--order", name])
            .unwrap()
            .command
        else {
            panic!("a states lookup");
        };
        assert_eq!(states.order, Some(order));
    }
    assert_eq!(
        refused(&["states", "a", "b", "--order", "up"]).kind(),
        ErrorKind::InvalidValue
    );
}

#[test]
fn a_window_takes_times_in_either_form() {
    let Command::States { window, .. } = parse(&[
        "states",
        "a",
        "b",
        "--since",
        "1793880000000",
        "--until",
        "2026-11-05T13:00:00+01:00",
    ])
    .unwrap()
    .command
    else {
        panic!("a states lookup");
    };
    assert_eq!(window.since, window.until);
    assert_eq!(
        refused(&["states", "a", "b", "--since", "yesterday"]).kind(),
        ErrorKind::ValueValidation
    );
}

#[test]
fn references_take_no_option_that_the_service_refuses_for_them() {
    let Command::Refs {
        ref_type,
        domain_id,
        service,
        ..
    } = parse(&["refs", "order_id", "ORD 1", "--service", "checkout"])
        .unwrap()
        .command
    else {
        panic!("a refs lookup");
    };
    assert_eq!(
        (ref_type.as_str(), domain_id.as_str()),
        ("order_id", "ORD 1")
    );
    assert_eq!(service.as_deref(), Some("checkout"));
    for option in ["--order", "--where", "--meta"] {
        let error = refused(&["refs", "order_id", "ORD-1", option, "x"]);
        assert_eq!(error.kind(), ErrorKind::UnknownArgument, "{option}");
    }
}

#[test]
fn the_help_of_each_command_can_be_built() {
    Cli::command().debug_assert();
}

#[test]
fn a_label_is_trimmed_after_the_split() {
    let Command::States { states, .. } = parse(&["states", "a", "b", "--meta", " region = e u "])
        .unwrap()
        .command
    else {
        panic!("a states lookup");
    };
    assert_eq!(states.meta, [("region".to_string(), "e u".to_string())]);
    assert_eq!(
        refused(&["states", "a", "b", "--meta", "  = eu"]).kind(),
        ErrorKind::ValueValidation
    );
}
