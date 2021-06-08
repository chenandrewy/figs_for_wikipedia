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


# ==== PROCESS COMPQ ====
nfirm = Inf
temp0 = compq
tempdsf0 = dsf

## find largest firms
tempdsf1 = tempdsf0 %>%
  mutate(year = year(date), month = month(date) ) %>%
  filter(month == 12) %>%
  group_by(permno, year) %>%
  arrange(permno, date) %>%    
  filter(row_number() == n())

tempdsf2 = tempdsf1 %>%
  transmute(
    permno
    , year = year + 1
    , me_lastyear = prc/pmax(cfacpr,1)*shrout/1e6
  ) %>%
  group_by(year) %>%    
  slice_max(me_lastyear,n=nfirm)

temp1 = temp0 %>%
  mutate(year = year(rdq)) %>%
  inner_join(
    tempdsf2
    , by = c('permno','year')
  ) 


## add lagged data
temp2 = temp1 %>%
  mutate(
    year = year(datadate)
    , q = quarter(datadate)
    , yearlag = year - 1
  )

temp3 = temp2 %>%    
  left_join(
    temp2 %>% transmute(
      gvkey
      , year
      , q                    
      , ibqlag = ibq
    )
    , by = c('gvkey','yearlag' = 'year','q')
  ) %>%
  mutate(
    dibq = 100*(ibq/ibqlag - 1)
  ) %>%
  filter(!is.na(dibq)) %>%
  select(permno, datadate, rdq, dibq)

compq2 = temp3

### DO STUFF
tempdsf0 = dsf %>%
  transmute(
    permno, date, prc_adj = prc/pmax(cfacpr,1)*shrout
  )
tempcomp0 = compq2 %>%
  mutate(rdq = rdq + 1) # XXX adjust for overnight, etc?
tempff0 = ff %>%   select(date, mkt_p)

# daywindow is in calendar days not trading days
daywindow = 365

temp0 = tempcomp0 %>%
  left_join(
    tempdsf0
    , by = c('permno')
  ) %>%
  filter(
    date <= rdq + daywindow
    , date >= rdq - daywindow
    , !is.na(dibq)
  ) %>%
  select(permno, rdq, dibq, date, prc_adj) %>%
  left_join(
    tempff0
    , by = c('date')
  )

temp1 = temp0 %>%
  arrange(permno, rdq, date) %>%
  group_by(permno,rdq) %>%
  mutate(tradeday = row_number())

temp2 = temp1 %>%
  left_join(
    temp1 %>%
      filter(date == rdq) %>%
      transmute(permno
                , rdq
                , tradeday_rdq = tradeday
                , prc_rdq = prc_adj
                , mkt_p_rdq = mkt_p
      )         
    , by = c('permno','rdq')
  ) %>%
  mutate(
    eventdate = tradeday - tradeday_rdq
    , cret = (prc_adj/prc_rdq)*100
    , mkt_cret = mkt_p/mkt_p_rdq*100
    , cret_adj = cret/mkt_cret*100
  )

## temp3 = temp2 %>%
##     mutate(
##         group = case_when(
##             dibq < 0 ~ 0
##           , dibq >= 0 ~ 1
##         ) %>% as.factor       
##     )

temp3 = temp2 %>%
  mutate(
    group = findInterval(dibq, c(-Inf,-10, 10, Inf))
  )

temp3 = temp2 %>%
  mutate(
    group = findInterval(dibq, c(-Inf,0, Inf))
  )


temp3 %>% filter(eventdate == 0) %>% group_by(group) %>% summarize(n())

temp2 %>% group_by(eventdate) %>% summarize(n()) %>% print(n = 100)


xxx


library(ggplot2)
library(hrbrthemes)
library(viridis)

# plot cret with errors 
tempsum =  temp3 %>%
  mutate( y0 = cret) %>%
  group_by(group, eventdate) %>%
  summarize(
    y = mean(y0, na.rm=T)
    , ub = mean(y0, na.rm=T) + 2*sd(y0, na.rm=T)/sqrt(n())
    , lb = mean(y0, na.rm=T) - 2*sd(y0, na.rm=T)/sqrt(n())                
    , n = n()
  ) %>%
  filter(
    n>30
    , abs(eventdate) <= 100
  ) %>%
  mutate(
    group = factor(group, levels = c(2,1), labels = c('Positive','Negative'))
  )

ggplot(tempsum, aes(x=eventdate)) + 
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

ggsave('ball_brown.pdf')
