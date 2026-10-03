//! The README's quick-start example, compiled.
//!
//! The README is the crate's front door and is not doctested (it is rendered by
//! crates.io and GitHub, not by rustdoc), so its one code block lives here too —
//! if the API moves, this fails to build rather than the README quietly going
//! stale. Keep the two in step.

use std::sync::Arc;

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{Context, Domain, Query, Record, Resource, Result};

#[derive(Resource, Default, Debug)]
#[resource(name = "note")]
struct Note {
    #[attribute(primary_key)]
    id: Option<String>,
    title: String,
}

async fn run() -> Result<()> {
    // Default-deny is the default; this domain is gated elsewhere, so it says so.
    let domain = Domain::builder().register::<Note>().permissive().build();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let note = Note::create(
        &domain,
        &mut ctx,
        Record::from_iter([("title", "buy milk")]),
    )
    .await?;
    let all = Note::read(&domain, &mut ctx, Query::new("note")).await?;

    println!("{} of {} notes", note.title, all.len());
    Ok(())
}

#[tokio::test]
async fn the_readme_example_runs() {
    run().await.expect("the front-door example works");
}
