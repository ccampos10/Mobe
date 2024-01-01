use polars::prelude::*;
use std::path::Path;
use std::fs;
use std::collections::HashMap;
use std::io::stdin;
use std::env;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn help(){
    println!("Uso:");
    println!("    -r            Leer datos de un csv");
    println!("    -e            Mostrar estadisticas de un ano");
    println!("    -s            Mostrar todos los datos almasenados en un ano");
    println!("    -v            Mostar la version del programa");
    println!("    -h            Mostar la ayuda");
}

fn new_df() -> DataFrame{
    df![
        "Id" => [] as [u64; 0],
        "Mes" => [] as [u32; 0],
        "Dia" => [] as [u32; 0],
        "Des" => [] as [&str; 0],
        "Descripcion" => [] as [&str; 0],
        "Cargo" => [] as [i32; 0],
        "Abono" => [] as [i32; 0],
        "Saldo" => [] as [i32; 0],
    ].unwrap()
}

fn read(path: &String) -> DataFrame{
    let mut schema = Schema::new();
    let _ = schema.insert_at_index(0, "Fecha".into(), DataType::Date);
    let _ = schema.insert_at_index(1, "Num Operacion".into(), DataType::Int64);
    let _ = schema.insert_at_index(2, "Descripcion".into(), DataType::Utf8);
    let _ = schema.insert_at_index(3, "Cargos".into(), DataType::Utf8);
    let _ = schema.insert_at_index(4, "Abonos".into(), DataType::Utf8);
    let _ = schema.insert_at_index(5, "Saldo".into(), DataType::Utf8);

    let df: DataFrame;
    match CsvReader::from_path(path) {
        Ok(csv_reader) => {
            match csv_reader.with_skip_rows(13)
            .with_try_parse_dates(true)
            .truncate_ragged_lines(true)
            .with_schema(Some(Arc::new(schema)))
            .finish() {
                Ok(_df) => df = _df,
                Err(err) => panic!("{}", err)
            }
        },
        Err(err) => panic!("No se pudo leer la ruta {}. {}", path, err)
    }

    let mut inx: usize = 0;
    let mut flag: bool = false;
    for e in df[0].iter(){
        if e.dtype() == DataType::Unknown{ flag = true; }
        else if !flag {
            inx += 1;
        }
    }

    df.head(Some(inx))
}

fn get_csv(ano: i32) -> DataFrame{
    let mut schema = Schema::new();
    let _ = schema.insert_at_index(0, "Id".into(), DataType::UInt64);
    let _ = schema.insert_at_index(1, "Mes".into(), DataType::UInt32);
    let _ = schema.insert_at_index(2, "Dia".into(), DataType::UInt32);
    let _ = schema.insert_at_index(3, "Des".into(), DataType::Utf8);
    let _ = schema.insert_at_index(4, "Descripcion".into(), DataType::Utf8);
    let _ = schema.insert_at_index(5, "Cargo".into(), DataType::Int32);
    let _ = schema.insert_at_index(6, "Abono".into(), DataType::Int32);
    let _ = schema.insert_at_index(7, "Saldo".into(), DataType::Int32);

    let mut path = Path::new(".").to_path_buf();
    path = path.join(env::args().collect::<Vec<_>>().get(0).unwrap());
    match path.parent(){
        None => panic!("{}, path not have a parent", path.display()),
        Some(s) => path = s.to_path_buf(),
    }
    path = path.join("data");
    if !path.exists() {
        match fs::create_dir(&path){
            Ok(t) => println!("path createt in {:?}", t),
            Err(e) => panic!("cant be create the path {}. {}", path.display(), e),
        }
    } 
    path = path.join(ano.to_string() + ".csv");
    match CsvReader::from_path(path){
        Ok(t) => {
            match t.with_try_parse_dates(true)
            .with_schema(Some(Arc::new(schema)))
            .finish(){
                Ok(df) => {
                    println!("{}", df);
                    df
                },
                Err(e) => panic!("{}", e),
            }
        },
        Err(_) => {
            let df = new_df();
            println!("{}", df);
            df
        },
    }
}

fn save_db(mut db: HashMap<i32, DataFrame>){
    for (ano, mut df) in db.iter_mut(){
        let mut path = Path::new(".").to_path_buf();
        path = path.join(env::args().collect::<Vec<_>>().get(0).unwrap());
        match path.parent(){
            None => panic!("{}, path not have a parent", path.display()),
            Some(s) => path = s.to_path_buf(),
        }
        path = path.join("data");
        if !path.exists() {
            match fs::create_dir(&path){
                Ok(t) => println!("path createt in {:?}", t),
                Err(e) => panic!("cant be create the path {}. {}", path.display(), e),
            }
        }
        path = path.join(ano.to_string() + ".csv");
        match fs::File::create(path){
            Ok(file) => {
                let _ = CsvWriter::new(file)
                    .include_header(true)
                    .with_separator(b',')
                    .finish(&mut df);
            },
            Err(err) => panic!("Un error salvaje a aparecido. {}", err)
        }
    }
}

fn generar_hash(value: Vec<AnyValue<'_>>) -> u64 {
    let mut hash = DefaultHasher::new();
    value.hash(&mut hash);
    hash.finish()
}

fn existe(id: u64, mut ids: Series) -> bool{
    ids = ids.sort(false);
    let mut left: usize = 0;
    let mut right: usize = ids.len();
    while left != right{
        let mid = (left+right)/2 as usize;
        let val: u64 = ids.get(mid).unwrap().try_extract().unwrap();
        if val == id {
            return true
        }
        else if val < id {
            left = mid+1;
        }
        else {
            right = mid;
        }
    }
    false
}

fn read_data(path: String){
    let data = read(&path);
    let count_row = data[0].len();
    let mut db: HashMap<i32, DataFrame> = HashMap::new();
    let mut flag = false;

    for i in (0..count_row).rev() {
        let ano = data[0].year().expect("Ok").get(i).unwrap();
        let mes = data[0].month().expect("Ok").get(i).unwrap();
        let dia = data[0].day().expect("Ok").get(i).unwrap();
        let des = data[2].get(i).unwrap();
        let cargo: i32 = data[3].get(i).unwrap().get_str().unwrap().replace(".", "").parse().unwrap();
        let abono: i32 = data[4].get(i).unwrap().get_str().unwrap().replace(".", "").parse().unwrap();
        let _df_ano: &DataFrame;
        match db.get(&ano) {
            Some(val) => _df_ano= val,
            None => {
                db.insert(ano, get_csv(ano));
                _df_ano= db.get(&ano).unwrap();
            }
        }

        let mut saldo: i32 = cargo + abono;
        if _df_ano[0].len() != 0{
            saldo += _df_ano[7].get(0).unwrap().try_extract::<i32>().unwrap();
        }
        else {
            let ano_ant: &mut DataFrame;
            match db.get_mut(&(ano - 1)) {
                Some(val) => ano_ant = val,
                None => {
                    db.insert(ano-1, get_csv(ano-1));
                    ano_ant = db.get_mut(&(ano-1)).unwrap();
                }
            }
            let mut saldo_ano_ant: i32 = 0;
            match ano_ant[7].get(0) {
                Ok(val) => saldo_ano_ant = val.try_extract().unwrap(),
                Err(_) => {}
            }
            let prov_df: DataFrame = df![
                "Id" => [1u64],
                "Mes" => [13u32],
                "Dia" => [0u32],
                "Des" => ["Sierre de ano" as &str],
                "Descripcion" => ["Sierre de ano" as &str],
                "Cargo" => [0i32],
                "Abono" => [0i32],
                "Saldo" => [saldo_ano_ant],
            ].unwrap();

            match prov_df.vstack(&ano_ant){
                Ok(v) => *ano_ant = v,
                Err(err) => panic!("Los datos guardados estan corrompidos. {}", err),
            }
            saldo += saldo_ano_ant;
        }

        //=============================
        //Hacer sin la funcion new_df()
        //=============================

        let mut prov_df: DataFrame = new_df();
        prov_df = prov_df.vstack(&df![
            "Id" => [1u64],
            "Mes" => [mes],
            "Dia" => [dia],
            "Des" => [des.get_str().unwrap()],
            "Descripcion" => ["tt"],
            "Cargo" => [cargo],
            "Abono" => [abono],
            "Saldo" => [1i32],
        ].unwrap()).unwrap();

        let id = generar_hash(prov_df.get(0).unwrap());
        prov_df = prov_df.clear();
        let df_ano: &mut DataFrame = db.get_mut(&ano).unwrap();

        //=======================================================================
        //       existe un caso en el que existe operasiones similares
        // y el hash generado es identico, por lo que no se ingresa la operacion
        //=======================================================================

        if existe(id, df_ano[0].clone()){
            if flag { panic!("Error. Parese que hay elementos repetidos y en conflicto.\nElemento num: {}", i) }
            continue
        }
        if !flag{ flag = true; }

        let mut descripcion = String::new();
        println!("Agregando una nueva entrada...");
        println!("Antecedentes");
        println!("{dia}-{mes}-{ano}");
        println!("Cargo: {}, Abono: {}", cargo, abono);
        println!("{}", des.get_str().unwrap());
        println!("Ingrese una descriocion");
        match stdin().read_line(&mut descripcion){
            Ok(_) => {},
            Err(err) => panic!("Ocurrio un error al leer la entrada, {}", err)
        }
        descripcion = descripcion.replace("\n", "");
        if df_ano[0].len() != 0{
            let ultimo_mes: u32 = df_ano[1].get(0).unwrap().try_extract().unwrap();
            let ultimo_dia: u32 = df_ano[2].get(0).unwrap().try_extract().unwrap();
            if ultimo_mes > mes {
                panic!("Se esta intentando agregar un elemento anterior al ultimo elemento ya agregado");
            }
            else if ultimo_mes == mes && ultimo_dia > dia {
                panic!("Se esta intentando agregar un elemento anterior al ultimo elemento ya agregado");
            }
        }

        prov_df = prov_df.vstack(&df![
            "Id" => [id],
            "Mes" => [mes],
            "Dia" => [dia],
            "Des" => [des.get_str().unwrap()],
            "Descripcion" => [descripcion],
            "Cargo" => [cargo],
            "Abono" => [abono],
            "Saldo" => [saldo],
        ].unwrap()).unwrap();

        match prov_df.vstack(&df_ano){
            Ok(v) => *df_ano = v,
            Err(err) => panic!("Los datos guardados estan corrompidos. {}", err),
        }
    }

    if flag{
        println!("Guardando datos...");
        //==============================================
        //     mostrar el ano de los csv cambiados
        //   esto implementarlo en la funcion save_db
        //==============================================
        save_db(db);
        println!("Datos guardados.");
    }
    else { println!("No se realizaron cambios."); }
}

fn estadisticas(ano: i32) -> Result<(), &'static str> {

    let df_ano = get_csv(ano);
    let count_row = df_ano[0].len();
    let mut mes: u32 = 0;
    let mut total_abono: i32 = 0;
    let mut count_abono: i32 = 0;
    let mut total_cargo: i32 = 0;
    let mut count_cargo: i32 = 0;
    let mut count_operaciones: i32 = 0;

    for i in (0..count_row).rev() {
        let mes_i: u32 = df_ano[1].get(i).unwrap().try_extract().unwrap();
        let abono_i: i32 = df_ano[6].get(i).unwrap().try_extract().unwrap();
        let cargo_i: i32 = df_ano[5].get(i).unwrap().try_extract().unwrap();
        if mes == 0 {
            mes = mes_i;
        }
        else if mes != mes_i{
            println!("Mes: {}", mes);
            println!("    Promedio de abonos: {}", if count_abono != 0 {total_abono/count_abono} else {0});
            println!("    Promedio de cargos: {}", if count_cargo != 0 {total_cargo/count_cargo} else {0});
            println!("    Numero de operaciones: {}", count_operaciones);
            println!("    Saldo: {}", total_abono+total_cargo);
            mes = mes_i;
            total_abono = 0;
            count_abono = 0;
            total_cargo = 0;
            count_cargo = 0;
            count_operaciones = 0;
        }
        if abono_i != 0 {
            total_abono += abono_i;
            count_abono += 1;
        }
        else if cargo_i != 0 {
            total_cargo += cargo_i;
            count_cargo += 1;
        }
        else if abono_i == 0 && cargo_i == 0{ return Err("Un error al interpretar los datos"); }
        else if abono_i != 0 && cargo_i != 0{ return Err("Un error al interpretar los datos"); }
        count_operaciones += 1;
    }
    println!("Mes: {}", mes);
    println!("    Promedio de abonos: {}", if count_abono != 0 {total_abono/count_abono} else {0});
    println!("    Promedio de cargos: {}", if count_cargo != 0 {total_cargo/count_cargo} else {0});
    println!("    Numero de operaciones: {}", count_operaciones);
    println!("    Saldo: {}", total_abono+total_cargo);
    
    Ok(())
}

fn show(ano: i32) -> Result<(), &'static str>{
    let df_ano = get_csv(ano);
    env::set_var("POLARS_FMT_MAX_ROWS", "-1");
    println!("{}", df_ano);
    env::set_var("POLARS_FMT_MAX_ROWS", "4");
    Ok(())
}

fn main() -> Result<(), &'static str>{
    //========================================
    //      Agregar el - a los comandos
    //========================================

    env::set_var("POLARS_FMT_MAX_ROWS", "4");
    env::set_var("POLARS_FMT_STR_LEN", "50"); 
    let args: Vec<String> = env::args().collect();
    if args.len() == 0 {
        return Err("Algo anda mal")
    }
    else if args.len() == 1 {
        help();
        println!();
        return Err("Los argumentos no son validos")
    }
    else {
        if args[1] == "-h" {
            help();
            return Ok(())
        }
        else if args[1] == "-v" {
            println!("Vercion: 0.1.0");
            return Ok(());
        }
        else if args[1] == "-r" {
            //==========================
            //   devolver un result
            //==========================

            //que pasa si args[2] no existe
            read_data(args[2].clone());
            return Ok(())
        }
        else if args [1] == "-e" { return estadisticas(args[2].clone().parse().unwrap()) }
        else if args[1] == "-s" { return show(args[2].clone().parse().unwrap()) }
        else if args[1] == "-t" {
            
            //===========================
            // Para hacer test y pruebas
            //===========================

            let num1 = 1;
            let num2 = 2;
            println!("{}", if num1 == 1 {123} else {321});
            println!("{}", if num2 == 1 {123} else {321});
            return Ok(())
        }
        else{
            help();
            return Err("Los argumentos no son validos")
        }
    }
}
