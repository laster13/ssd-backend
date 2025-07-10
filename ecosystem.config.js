module.exports = {
  apps: [
    {
      name: "backend",
      script: "./start.sh",
      interpreter: "bash",
      env: {
        FORCE_COLOR: "1",
        PYTHONUNBUFFERED: "1",
      },
      out_file: "data/logs/backend.out.log",      // fichier de log standard
      error_file: "data/logs/backend.err.log",    // fichier de log erreurs
      log_date_format: "",                         // ⛔ désactive l'horodatage PM2
      merge_logs: true,                            // fusionne les logs si plusieurs instances
    }
  ]
}
