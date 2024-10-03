use std::env;
use std::error::Error;
use csv::ReaderBuilder;
use csv::WriterBuilder;

#[derive(Debug)]
struct Detail {
    uut_record_detail_id: String,
    uut_record_id: String,
    date_time: String,
    test: String,
    result: String,
    value_name: String,
    value: String,
    min_set_point_name: String,
    min_set_point: String,
    max_set_point_name: String,
    max_set_point: String,
    units: String,
    elapsed_time: String,
}

#[derive(Debug)]
struct Record {
    uut_record_id: String,
    serial_no: String,
    model_no: String,
    date_time: String,
    system_id: String,
    op_id: String,
    test_type: String,
    test_result: String,
    test_port: String,
    details: Vec<Detail>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <input_file>", args[0]);
        return Ok(());
    }

    let input_file = &args[1];
    let output_file = format!("{}_processed.csv", input_file.trim_end_matches(".csv"));

    let mut rdr = ReaderBuilder::new().from_path(input_file)?;
    let mut wtr = WriterBuilder::new().from_path(output_file)?;

    let headers = rdr.headers()?.clone();
    wtr.write_record(&headers)?;

    let mut records: Vec<Record> = Vec::new();
    let mut current_record: Option<Record> = None;

    for result in rdr.records() {
        let record = result?;
        let record_id = record.get(0).unwrap_or("").to_string();
        let serial =  record.get(1).unwrap_or("").to_string();
        let model =  record.get(2).unwrap_or("").to_string();
        let datetime1 =  record.get(3).unwrap_or("").to_string();
        let system_id =  record.get(4).unwrap_or("").to_string();
        let op_id =  record.get(5).unwrap_or("").to_string();
        let test_type =  record.get(6).unwrap_or("").to_string();
        let test_result =  record.get(7).unwrap_or("").to_string();
        let test_port =  record.get(8).unwrap_or("").to_string();

        let detail_id = record.get(9).unwrap_or("").to_string();
        let record_id2 = record.get(10).unwrap_or("").to_string();
        let datetime2 = record.get(11).unwrap_or("").to_string();
        let test = record.get(12).unwrap_or("").to_string();
        let test_result2 = record.get(13).unwrap_or("").to_string();
        let value_name = record.get(14).unwrap_or("").to_string();
        let value = record.get(15).unwrap_or("").to_string();
        let min_set_point_name = record.get(16).unwrap_or("").to_string();
        let min_set_point = record.get(17).unwrap_or("").to_string();
        let max_set_point_name = record.get(18).unwrap_or("").to_string();
        let max_set_point = record.get(19).unwrap_or("").to_string();
        let units = record.get(20).unwrap_or("").to_string();
        let elapsed_time = record.get(21).unwrap_or("").to_string();

        if !record_id.is_empty() && !serial.is_empty() {
            if let Some(rec) = current_record.take() {
                records.push(rec);
            }
            current_record = Some(Record {
                uut_record_id: record_id,
                serial_no: serial,
                model_no: model,
                date_time: datetime1,
                system_id,
                op_id,
                test_type,
                test_result,
                test_port,
                details: vec![Detail {
                    uut_record_detail_id: detail_id,
                    uut_record_id: record_id2,
                    date_time: datetime2,
                    test,
                    result: test_result2,
                    value_name,
                    value,
                    min_set_point_name,
                    min_set_point,
                    max_set_point_name,
                    max_set_point,
                    units,
                    elapsed_time,
                }],
            });
        } else if let Some(ref mut rec) = current_record {
            rec.details.push(Detail {
                uut_record_detail_id: detail_id,
                uut_record_id: record_id2,
                date_time: datetime2,
                test,
                result: test_result2,
                value_name,
                value,
                min_set_point_name,
                min_set_point,
                max_set_point_name,
                max_set_point,
                units,
                elapsed_time,
            });
        }
    }

    if let Some(rec) = current_record {
        records.push(rec);
    }

    for record in records {
        if record.system_id == "PreCharge18450BR02" {
            if let Some(detail) = record.details.iter().find(|d| d.test == "TestMeasureDelay" && d.value_name == "Chamber Flow") {
                wtr.write_record(&[
                    &record.uut_record_id,
                    &record.serial_no,
                    &record.model_no,
                    &record.date_time,
                    &record.system_id,
                    &record.op_id,
                    &record.test_type,
                    &record.test_result,
                    &record.test_port,
                    &detail.uut_record_detail_id,
                    &detail.uut_record_id,
                    &detail.date_time,
                    &detail.test,
                    &detail.result,
                    &detail.value_name,
                    &detail.value,
                    &detail.min_set_point_name,
                    &detail.min_set_point,
                    &detail.max_set_point_name,
                    &detail.max_set_point,
                    &detail.units,
                    &detail.elapsed_time
                ])?;
            } else {
                wtr.write_record(&[
                    &record.uut_record_id,
                    &record.serial_no,
                    &record.model_no,
                    &record.date_time,
                    &record.system_id,
                    &record.system_id,
                    &record.test_type,
                    &record.test_result,
                    &record.test_port,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""
                ])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}
