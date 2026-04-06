# Lolo Music — Guitar Arbitrage System

## El negocio
Arbitraje de guitarras USA → México. Comprar usadas en EE.UU., vender en México con 30%+ margen.
Inspirado en Guitar's Home (guitars-home.com / @guitarshome).
**Estado actual:** Validación — fase inicial con 5 guitarras antes de escalar a 20+.
Capital disponible para ~20 guitarras, pero estrategia es validar primero.
Ivan quiere vender al mismo precio que GH — no hay factor de descuento.

## Fórmula de negocio
```
Landed cost     = precio_compra_USD + $150 USD (logística, sin IVA)
Benchmark venta = gh_price_mxn / usd_mxn  ← SIEMPRE este, nunca Reverb US
Margen          = (benchmark_usd - landed_cost) / landed_cost ≥ 30%
Margen real     = incluir comisión MercadoLibre 17% + empaque
```
Reverb Sold avg = referencia secundaria solamente (contexto, no para calcular margen).

## Reglas críticas de código
- Benchmark principal = GH listing price en MXN convertido a USD
- Si GH no tiene listing activo → usar gh_instagram_history.csv (1,225+ precios históricos)
- Reverb sold = solo referencia, nunca para calcular oportunidad
- Red flags (headstock repair, crack, etc.) = filtrar automáticamente
- Score mínimo fuzzy match = 75
- Price ratio sanity check = 0.45–2.00 (US price / GH USD equiv)
- Condición mínima aceptable = VG (Very Good) — Good o peor se filtra
- Una URL de US = una sola oportunidad (deduplicación por URL)

## Filtros de matching (matching.py)
- **Marca**: Gibson vs Gibson, nunca Epiphone vs Gibson
- **Familia de modelo**: Les Paul vs Les Paul, nunca SG vs Les Paul
- **Tier premium**: Custom Shop vs Custom Shop — no mezclar con standard
- **Acústica vs eléctrica**: SJ-200 nunca matchea con ES-339. Detectado por modelo.
- **Anniversary editions**: "70th Anniversary" requiere match en ambos lados
- **Año de producción**: diferencia > 5 años = mismatch. Vintage (≤1984) nunca matchea con moderno (≥2000)
- **Año reissue** (`extract_reissue_year`): si GH dice "1957 R7" y US dice "1968 Custom", gap ≥ 5 años → rechaza
- **Artistas/firma**: "Richie Kotzen Strat" no matchea "Player Strat"
- **One of a Kind / firmadas / numeradas (23/100)**: se saltan — precio no replicable
- **Florentine**: si GH dice "Florentine Plus" y US no → rechaza (modelo distinto, top esculpido)
- **Goldtop**: si GH dice "Goldtop" o "Gold Top" y US no → rechaza (finish específico, precio diferente)
- **Quilt/10-Top**: si GH declara quilt/10-Top y US no → rechaza (premium visual $300-800 USD)

## Lógica de benchmark y ajustes (main.py — evaluate_match)
- **All Original cap**: si el benchmark de Instagram es para una guitarra "All Original" pero el listing US no lo declara, se reemplaza el benchmark por el Reverb sold avg (si es menor). Razón: premium all-original es 30-50% sobre no-original → no asumir ese premium en el listing US.
- **Venta MX PROACTIVO**: prioridad GH activa → Instagram benchmark × FX → estimado 37% ROI
- **Reverb warnings**:
  - Reverb > benchmark × 1.05 → "⚠ REVERB > GH PRECIO — comps US más caros, verificar mismo spec"
  - Reverb < benchmark × 0.60 → "⚠ REVERB MUY BAJO — benchmark puede ser optimista"
  - 60-105% del benchmark → sin warning (rango normal)

## Stack técnico
- Python 3.14, virtualenv en `venv/`
- **Siempre activar:** `source venv/bin/activate`
- **Correr con output en tiempo real:** `python3 -u main.py 2>&1 | tee logs/main_test.log`
- Dependencias: requests, beautifulsoup4, lxml, rapidfuzz, python-dotenv, playwright
- Base de datos: SQLite (`price_history.db`)
- Notificaciones: Telegram Bot API (plain text, sin Markdown para evitar errores 400)

## Archivos del sistema

| Archivo | Propósito |
|---|---|
| `main.py` | Pipeline principal — scraping + matching + deal score + Telegram |
| `scrapers.py` | Todas las fuentes US + GH + Reverb |
| `matching.py` | Fuzzy match engine con todos los filtros de spec |
| `pricing.py` | calculate_landed_cost, calculate_net_margin, is_opportunity |
| `catalog.py` | Instagram catalog lookup + liquidity scores + proactive targets |
| `deal_score.py` | Score 0-100: BUY NOW / REVIEW / PASS + condición + oferta sugerida |
| `price_history.py` | SQLite — historial de precios + detección de drops |
| `monitor.py` | FX rate monitor + inventory tracker CLI + evaluador manual (`eval`) |
| `notifier.py` | Telegram — plain text, chunked a 4096 chars |
| `instagram_fetch.py` | Playwright scraper @guitarshome (4,189 posts totales) |
| `instagram_parse.py` | Parser captions → gh_instagram_history.csv + gh_sold_catalog.csv |

## Archivos de datos generados
| Archivo | Contenido |
|---|---|
| `gh_instagram_history.csv` | 1,225+ posts con precio detectado — benchmark histórico MX |
| `gh_sold_catalog.csv` | Posts marcados como SOLD (vacío — GH elimina precio al vender) |
| `price_history.db` | SQLite con historial de precios US + días en venta (DOM acumula con cada cron) |
| `inventory.json` | Guitarras compradas y vendidas (CLI via monitor.py) |
| `fx_cache.json` | Cache del tipo de cambio |

## Fuentes de compra

| Fuente | Tipo | Estado | Notas |
|---|---|---|---|
| Dave's Guitar | Shopify JSON | Funcionando | Condición en descripción |
| Chicago Music Exchange | Shopify JSON | Funcionando | `vintage-used` + `price-drops`. 79% catálogo sold out — filtrar `available: True` |
| Cream City Music | BigCommerce HTML | Funcionando | Precio en aria-label del card |
| Music Go Round | BigCommerce HTML | Funcionando (pocos items) | Precio en `data-product-price` |
| Elderly Instruments | Shopify JSON | Funcionando | |
| Norman's Rare Guitars | Shopify JSON | Funcionando | ~165 items disponibles, vintage Gibson/Fender, 100% `avail:True` |
| Tone Shop Guitars | Shopify JSON | Funcionando | Addison TX, colecciones `used-electrics` + `vintage` |
| eBay | HTML scraper | DESACTIVADO — bot detection | |
| Guitar Center | HTML scraper | DESACTIVADO — bot detection | |
| Sam Ash | HTML scraper | DESACTIVADO — 403 | |
| Reverb | API JSON | Funcionando | ~2,700 listings activos |
| Guitar's Home | WooCommerce HTML | Funcionando | 69 guitarras activas |

**Nota CME:** `available: False` en Shopify JSON = sold out real (confirmado). Filtrar siempre.
**Nota Norman's:** Todos los productos son used/vintage, no requiere filtro de tags adicional.

## Deal Score (deal_score.py)
- 40 pts Margen | 20 pts Liquidez | 15 pts Match | 10 pts Oferta/Drop | 10 pts Días en venta (DOM) | 5 pts Fuente
- Penalización por condición: VG = -5pts, VG+ = -3pts, Good+ o peor = filtrado
- ≥75 → BUY NOW (alerta Telegram urgente separada)
- 50–74 → REVIEW
- <50 → PASS (silencioso)
- DOM ≥ 45 días → mostrar precio sugerido de oferta (90% del precio lista)
- **Nota DOM:** El componente de 10 pts de DOM empieza en 0 y mejora con semanas de cron corriendo. Deals que hoy son 65-73 llegarán a BUY NOW solos en 2-3 semanas.

## Modos del pipeline
- **REACTIVO**: GH tiene la guitarra activa en su web hoy → buscamos la misma más barata en EE.UU.
- **PROACTIVO**: Modelos que GH vende frecuentemente (de Instagram history) → buscamos activamente en EE.UU. aunque GH no la tenga listada hoy.

## Instagram @guitarshome
- 4,189 posts totales detectados
- ~2,033 posts válidos descargados, 1,225 con precio detectado. 249 posts vacíos (bloqueados) → eliminados.
- GH **no marca como "VENDIDO"** — usa `*** SOLD ***` y elimina el precio al vender
- El `gh_instagram_history.csv` (todos los posts con precio) es el benchmark real
- El sistema usa `full_history` para proactive targets y benchmark de fallback — NO `sold_catalog`
- Cron corre fetch a las 2am, parse a las 3am
- Cookies de Instagram expiran — renovar `INSTAGRAM_SESSION_ID` si fetch falla (posts vacíos)

## Scheduler (crontab instalado y activo)
```
0 0,6,12,18 * * *  main.py          — scan arbitraje cada 6 horas
0 9 * * *           monitor.py       — FX rate + alertas inventario
0 2 * * *           instagram_fetch  — resume descarga @guitarshome
0 3 * * *           instagram_parse  — actualiza CSVs
```
Logs en: `logs/main.log`, `logs/monitor.log`, `logs/instagram.log`

## Variables .env clave
```
USD_MXN          — actualizado automáticamente por monitor.py vía open.er-api.com
LOGISTICS_USD    = 150
MIN_MARGIN       = 0.30
MIN_MATCH_SCORE  = 75
PRICE_RATIO_MIN  = 0.45
PRICE_RATIO_MAX  = 2.00
GH_REVERB_SHOP   = (pendiente — slug de GH en Reverb)
INSTAGRAM_SESSION_ID / CSRF_TOKEN / DS_USER_ID — configurados (expiran, renovar si fetch falla)
```

## Canales de venta (prioridad)
1. MercadoLibre — comisión 17%, mayor volumen MX
2. Instagram — prueba social, DM sin comisión
3. Facebook Groups — "Compra Venta Guitarras México"
4. Reverb — fallback si no vende en MX en 30 días

## 5 guitarras de validación inicial
1. Fender American Professional II Strat — compra $950–1,100 USD → venta $30,000–34,000 MXN
2. Gibson Les Paul Standard 50s — compra $1,800–2,100 USD → venta $55,000–62,000 MXN
3. PRS CE 24 — compra $900–1,100 USD → venta $28,000–33,000 MXN
4. Gibson SG Standard — compra $900–1,100 USD → venta $27,000–32,000 MXN
5. Fender American Ultra Telecaster — compra $1,100–1,350 USD → venta $34,000–40,000 MXN

## Evaluador manual (monitor.py eval)
Para evaluar una guitarra específica que encuentres en línea sin correr el pipeline completo:
```bash
python3 monitor.py eval
```
- Pide: título, precio, condición, fuente, URL
- Busca benchmark en GH activo → Instagram history (fuzzy ≥ 72)
- Calcula landed cost, margen, precio mínimo de venta para 30%
- Muestra warnings: ALL ORIGINAL, si es guitarra objetivo de validación (★)
- Veredictos: OPORTUNIDAD / REVISAR / PASS

## Pendientes técnicos
- [ ] Obtener slug de Reverb de Guitar's Home para weighted benchmark
- [ ] Terminar Instagram fetch (faltan ~2,156 posts de 4,189)
- [ ] Benchmark MercadoLibre (Fase 2 — después de primera venta)
- [ ] Feedback loop: registrar precio real de venta vs benchmark estimado
- [ ] Investigar por qué CME (415 items) no produce ningún match — posible issue de títulos

## Pendientes de negocio
- [ ] Crear cuenta MercadoLibre como vendedor
- [ ] Abrir cuenta Shipito o MyUS (dirección en EE.UU.)
- [ ] Elegir nombre e Instagram de la tienda
- [ ] Primera compra cuando Deal Score ≥ 65 en alguna de las 5 guitarras objetivo

## Guitarras a NO comprar (negocio, no técnico)
- Headstock roto/reparado — el mercado MX castiga 25-40%, no vale la pena en Fase 1
- Condición "Good" o peor — riesgo de no vender o vender con descuento
- "One of a Kind", firmadas, numeradas — precio no replicable, ya filtradas en código
- All Original vintage sin confirmar originalidad — benchmark inflado 30-50% vs no-original

## Calidad del reporte actual
- **Calificación alcanzada: 9/10**
- Todos los false positives conocidos filtrados por código
- El último punto (10/10) requiere DOM acumulado (~2-3 semanas de cron) para que scores suban a BUY NOW
- Fixes aplicados en esta sesión: All Original cap, Goldtop filter, Florentine filter, Reissue year check, Reverb warning thresholds, Venta MX PROACTIVO

## Comandos útiles
```bash
# Activar entorno
source venv/bin/activate

# Correr sistema completo (con output en tiempo real)
python3 -u main.py 2>&1 | tee logs/main_test.log

# Solo monitor FX + inventario
python3 monitor.py

# Ver inventario
python3 monitor.py list

# Agregar guitarra comprada
python3 monitor.py add

# Marcar vendida
python3 monitor.py sell <id>

# Evaluar guitarra manualmente
python3 monitor.py eval

# Continuar descarga Instagram
python3 instagram_fetch.py

# Parsear posts descargados
python3 instagram_parse.py

# Verificar logs del cron
tail -50 logs/main.log

# Test deal score
python3 deal_score.py
```

## Preferencias de trabajo con Claude
- Conversación en español, código en inglés
- Respuestas directas y accionables, sin relleno
- Explicar el "por qué" de negocio cuando hay decisiones técnicas con impacto comercial
- No construir más infraestructura hasta tener la primera venta validada
- Al analizar reportes: identificar el bug/false positive exacto y el fix de código concreto, no solo describirlo
