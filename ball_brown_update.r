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
dateend = "'2015-12-31'"

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


# ==== PROCESS DATA: MERGE EARNINGS SURPRISES ONTO DAILY RETURNS ====
tempcomp = compq
tempdsf = dsf 

# find earnings surprises
tempcomp = tempcomp %>% 
  group_by(permno) %>% 
  arrange(permno, rdq) %>% 
  mutate(
    dibq = 100*(ibq/lag(ibq,4)-1)
  ) %>% 
  select(permno,rdq, dibq) %>% 
  filter(!is.na(dibq))

# find ew index
tempbench = tempdsf %>% 
  group_by(date) %>% 
  summarise(
    ret_all = mean(ret, na.rm=T)
  )

# find p (price index, cum ret less benchbmark)
tempdsf = tempdsf %>% 
  filter(!is.na(ret)) %>% 
  group_by(permno) %>% 
  arrange(permno, date) %>% 
  mutate(
    p = cumprod(1+ret-ret_all) 
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
  mutate(
    row = row_number()
    , row_rdq = if_else(date==rdq_last, row, NA_integer_)
    , cret_rdq = if_else(date==rdq_last, p, NA_real_)
  ) %>% 
  fill(row_rdq) %>% 
  fill(cret_rdq) %>% 
  mutate(
    tdays_since_rdq = row - row_rdq
    , cret_since_rdq = 100*(p / cret_rdq - 1)
  ) %>% 
  select(permno,date,rdq_last,dibq,tdays_since_rdq,cret_since_rdq) %>% 
  filter(
    tdays_since_rdq <= 60
  )


# group returns
tempsum = tempdsf %>% 
  mutate(
    group = if_else(dibq>0, 'pos', 'neg')
  ) %>% 
  group_by(group, tdays_since_rdq) %>% 
  summarize(
    y = mean(cret_since_rdq, na.rm=T)
    , ub = mean(cret_since_rdq, na.rm=T) + 2*sd(cret_since_rdq)/sqrt(n())
    , lb = mean(cret_since_rdq, na.rm=T) - 2*sd(cret_since_rdq, na.rm=T)/sqrt(n())                
    , n = n()
  ) %>%
  filter(
    tdays_since_rdq <= 90
  ) %>%
  mutate(
    group = factor(group, levels = c('pos','neg'), labels = c('Positive','Negative'))
  )


ggplot(tempsum, aes(x=tdays_since_rdq)) + 
  geom_line(aes(y = y, group = group, color = group)) +
  geom_line(aes(y = ub, group = group, color = group), linetype = 'dashed') +
  geom_line(aes(y = lb, group = group, color = group), linetype = 'dashed') +
  scale_color_manual(name="Earnings announcement",values=c("Blue","Red")) +
  xlab('Trading days relative to earnings announcement') +
  ylab("Stock price") +
  theme_bw() +
  theme(
    legend.position = c(0.8, 0.2)
    , panel.grid.major = element_blank()
    , panel.grid.minor = element_blank()
    , panel.border = element_blank()
    , axis.line = element_line(color = 'black')
    , legend.box.background = element_rect(color = 'black')
    , axis.title.x = element_text(size=14)
    , axis.title.y = element_text(size=14)
    , legend.text = element_text(size=12)
    , legend.title = element_text(size=14)
  )     

# ggsave('ball_brown.pdf')
