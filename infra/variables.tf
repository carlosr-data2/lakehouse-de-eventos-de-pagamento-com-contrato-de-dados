variable "project" {
  description = "Prefixo usado em todos os nomes de recurso"
  type        = string
  default     = "evt-lakehouse"
}

variable "enable_glue" {
  description = "Cria Glue Catalog/Crawler/Jobs (requer LocalStack Pro)"
  type        = bool
  default     = false
}

variable "enable_lifecycle" {
  description = "Cria regras de ciclo de vida no S3. O LocalStack community grava a regra mas nao devolve na leitura os campos que o provider usa para confirmar convergencia, entao o apply estoura por timeout. Desligado localmente, ligado na AWS real."
  type        = bool
  default     = false
}

variable "max_reject_rate" {
  description = "Taxa maxima de rejeicao aceita pelo quality gate"
  type        = number
  default     = 0.05
}
