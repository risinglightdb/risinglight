use risinglight::Database;
use std::io::Write;

fn main() {
    let db = Database::new();
    loop {
        print!("> ");
        std::io::stdout().lock().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let ret = db.run(&input);
        println!("{:?}", ret);
    }
}
