use polars::{lazy::dsl::SpecialEq, prelude::*};

fn main() -> PolarsResult<()> {
    // 创建一个 DataFrame
    let mut df = df![
        "keys" => ["a", "b", "c", "d", "e"],
        "values" => [1, 2, 3, 4, 5],
    ]?;

    df.apply("values", multiply_by_two)?;

    let df = df
        .lazy()
        .with_column(col("keys").map(with_prefix_k, SpecialEq::from_type(DataType::String)))
        .collect()?;

    // 打印 DataFrame
    println!("{:?}", df);

    Ok(())
}

// 定义一个 UDF，将输入值乘以 2
fn multiply_by_two(s: &Series) -> Series {
    let s = s.i32().expect("Expected type to be i32");
    let s = s.apply(|value| value.map(|v| v * 2));
    s.into_series()
}

fn with_prefix_k(s: Series) -> PolarsResult<Option<Series>> {
    let out: StringChunked = s
        .str()
        .expect("as str")
        .apply_generic(|v| v.map(|vv| format!("k-{vv}")));
    //.into_series();
    Ok(Some(out.into_series()))
}
