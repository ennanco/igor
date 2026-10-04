use std::{
    error::Error,
    fs,
    io::{self, Read},
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut context = String::new();
    io::stdin().read_to_string(&mut context)?;
    if !context.contains("\"stdout_path\"") {
        return Err("missing extractor input".into());
    }
    let stdout = fs::read("stdout.log")?;
    fs::write(
        "native-metrics.txt",
        format!("stdout bytes: {}\n", stdout.len()),
    )?;
    println!(
        r#"{{"schema_version":1,"status":"success","metrics":{{"native bytes":{},"native score":0.9375}},"artifacts":[{{"path":"native-metrics.txt","role":"metrics"}}]}}"#,
        stdout.len()
    );
    Ok(())
}
