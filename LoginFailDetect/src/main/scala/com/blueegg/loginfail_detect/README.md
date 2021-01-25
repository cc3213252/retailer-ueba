## LoginFail代码的问题

1、比如1秒就出现了两次失败，就应该报警  
2、没有考虑乱序  

## LoginFailAdvance代码问题  

1、登陆失败次数可扩展性差了
2、没有考虑乱序    

## cep

LoginFailAdvance基础上考虑乱序问题的话，代码太复杂了，故引入cep库  
适合处理复杂事件