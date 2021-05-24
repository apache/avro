#[derive(Debug, serde::Serialize)]
struct Test {
    a: i64,
    b: &'static str,
}

fn main() -> anyhow::Result<()> {
    let test = Test { a: 27, b: "foo" };
    let value = avro_rs::to_value(test)?;
    println!("{:?}", value);
    Ok(())
}
