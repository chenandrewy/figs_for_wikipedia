# ==== LOG IN TO WRDS ====

library(RPostgres)
library(getPass)

username = getPass(msg = 'wrds user')
pass = getPass(msg = 'password')

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  user=username,
                  password=pass,
                  sslmode='require')




# ==== DOWNLOAD DATA ====
options(stringsAsFactors = FALSE)
library(tidyverse)
library(readxl)
library(lubridate)

numRowsToPull = -1  # Set to -1 to get all data, set to positive value for testing

datebegin = "'2010-01-01'"
dateend = "'2020-12-31'"

### DOWNLOAD EARNINGS DATA AND ADD PERMNOS
res <- dbSendQuery(wrds, paste0("
select a.gvkey, a.datadate, a.conm, a.saleq, a.ibq, a.rdq, a.mkvaltq, a.fic
from comp.fundq as a
where a.fic = 'USA' and a.datadate >=  ", datebegin
  , " and a.datadate <= ", dateend
))
temp <- dbFetch(res) %>% as_tibble
dbClearResult(res)

res <- dbSendQuery(
  wrds, "
select
  b.gvkey, b.linkprim, b.linktype, b.liid
  , b.lpermno, b.linkdt, b.linkenddt
from crsp.ccmxpf_lnkhist as b
"
)
temp2 <- dbFetch(res)
dbClearResult(res)

compq = temp %>% left_join(temp2, by = c('gvkey')) %>%
  mutate(
    linkenddt = if_else(is.na(linkenddt),as.Date('3000-01-01'),linkenddt)
  ) %>%
  filter(
    linktype %in% c('LC','LU')
    , linkprim %in% c('P','C')
    , datadate >= linkdt
    , datadate <= linkenddt
  ) %>%
  transmute(
    gvkey
    , permno = lpermno
    , datadate
    , mkvaltq
    , rdq
    , ibq
    , conm
  )

### DOWNLOAD DSF
# takes maybe 5 minutes
res <- dbSendQuery(wrds, paste0(
  "
select permno, date, ret
from dsf 
where  date >= " , datebegin
  , " and date <= ", dateend
))
temp <- dbFetch(res) %>% as_tibble
dbClearResult(res)

dsf = temp

### DOWNLOAD S&P 500
res <- dbSendQuery(wrds, paste0(
  "
select *
from dsp500
where  caldt >= " , datebegin
  , " and caldt <= ", dateend
))
temp <- dbFetch(res) %>% as_tibble
dbClearResult(res)

sp500 = temp %>% 
  transmute(
    date = caldt
    , ret_sp = spindx / lag(spindx,1) -1
  )

# ==== PROCESS DATA: MERGE EARNINGS SURPRISES ONTO DAILY RETURNS ====
tempcomp = compq 
tempdsf = dsf 

# filter approximate S&P 500
# (approximates size/bm adjustments)
# tempcomp = tempcomp %>% 
#   mutate(yearq = year(rdq)*10+quarter(rdq)) %>% 
#   group_by(yearq) %>% 
#   arrange(yearq, desc(mkvaltq)) %>% 
#   mutate(rank = row_number()) %>% 
#   filter(rank <= 500)


# find earnings surprises
tempcomp = tempcomp %>% 
  group_by(permno) %>% 
  arrange(permno, rdq) %>% 
  mutate(
    dibq = 100*(ibq/lag(ibq,4)-1)
  ) %>% 
  select(permno,rdq, dibq) %>% 
  filter(!is.na(dibq))


# find p (price index, cum ret less benchbmark)
tempdsf = tempdsf %>% 
  left_join(sp500, by='date') %>% 
  filter(!is.na(ret)) %>% 
  group_by(permno) %>% 
  arrange(permno, date) %>% 
  mutate(
    p_adj = cumprod(1+ret-ret_sp) 
    , p = cumprod(1+ret) 
  )

# join, keeping all returns
tempdsf = tempdsf %>% 
  left_join(tempcomp, by = c('permno','date'='rdq')) %>% 
  group_by(permno) %>% 
  arrange(permno,date) %>% 
  mutate(
    rdq_last = if_else(!is.na(dibq), date, NA_Date_)
  ) %>% 
  fill(rdq_last) %>% 
  fill(dibq) %>% 
  filter(!is.na(dibq))

dsf2 = tempdsf



# ==== EVENT STUDY ====

library(ggplot2)
library(hrbrthemes)
library(viridis)

tempdsf = dsf2

# stock level buy-hold returns
tempdsf = tempdsf %>% 
  mutate(p_select = p) %>% 
  mutate(
    row = row_number()
    , row_rdq = if_else(date==rdq_last, row, NA_integer_)
    , p_rdq = if_else(date==rdq_last, p_select, NA_real_)
  ) %>% 
  fill(row_rdq) %>% 
  fill(p_rdq) %>% 
  mutate(
    tdays_since_rdq = row - row_rdq
    , cret_since_rdq = 100*(p_select / p_rdq - 1)
  ) %>% 
  select(permno,date,rdq_last,dibq,tdays_since_rdq,cret_since_rdq) %>% 
  filter(
    tdays_since_rdq <= 60
  )

# group returns
tempsum = tempdsf %>% 
  mutate(
    group = case_when(
    dibq < 0 ~ 'neg'
    , dibq > 0  ~ 'pos'
    )
  ) %>% 
  group_by(group, tdays_since_rdq) %>% 
  summarize(
    y = mean(cret_since_rdq, na.rm=T)
    , ub = mean(cret_since_rdq, na.rm=T) + 2*sd(cret_since_rdq, na.rm=T)/sqrt(n())
    , lb = mean(cret_since_rdq, na.rm=T) - 2*sd(cret_since_rdq, na.rm=T)/sqrt(n())                
    , n = n()
  ) %>% 
  filter(!is.na(group))

tempbench = tempdsf %>% 
  group_by(tdays_since_rdq) %>% 
  summarize(
    y_bench = mean(cret_since_rdq, na.rm=T)
  ) 

tempsum = tempsum %>% 
  left_join(tempbench) %>% 
  mutate(
    y = y - y_bench
    , ub = ub - y_bench
    , lb = lb - y_bench
  ) 



ggplot(tempsum, aes(x=tdays_since_rdq)) + 
  geom_line(aes(y = y, group = group, color = group)) +
  geom_line(aes(y = ub, group = group, color = group), linetype = 'dashed') +
  geom_line(aes(y = lb, group = group, color = group), linetype = 'dashed') +
  scale_color_manual(name="Earnings announcement",values=c("Blue",'Red')) +
  xlab('Trading days relative to earnings announcement') +
  ylab("Cumulative Abnormal Return (%)") +
  theme_bw() +
  theme(
    legend.position = c(0.2, 0.2)
    , panel.grid.major = element_blank()
    , panel.grid.minor = element_blank()
    , panel.border = element_blank()
    , axis.line = element_line(color = 'black')
    , legend.box.background = element_rect(color = 'black')
    , axis.title.x = element_text(size=14)
    , axis.title.y = element_text(size=14)
    , legend.text = element_text(size=12)
    , legend.title = element_text(size=14)
  ) +
  ylim(-1.5,1.0)

# ggsave('ball_brown.pdf')
