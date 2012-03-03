## Writes to the positions table in the specified MySQL database. The
## input is a data.frame, which follows the positions table:
## | Symbol |  Date | Strategy | Indicator | Details
##
## The connection can be accomplished either by passing a database name,
## a user and a password, or by passing a group defined in .my.cnf file.
## If group is passed, then it's used and user/password are ignored.
##
## @param data.frame positions The positions to write to the database
## @param string dbName The database name
## @param string user The database user
## @param string password
## @param string group The group referring to my.cnf
writePositionsToDb = function( positions, dbName, user, password, group )
{
   require( RMySQL )

   conn = NULL

   if( missing( group ) )
   {
      conn = tryCatch(
                  dbConnect( MySQL(), dbname=dbName, user=user, password=password ),
                  error = function( ee ) NULL,
                  warning = function( ww ) NULL )
   }
   else
   {
      conn = tryCatch(
                  dbConnect( MySQL(), group=group ),
                  error = function( ee ) NULL,
                  warning = function( ww ) NULL )
   }

   if( !is.null( conn ) )
   {
      for( ii in 1:NROW(positions) )
      {
         pp = positions[ii,]
         qq = paste( sep="",
                     "insert into positions (symbol, date, strategy, indicator, details) values ('",
                     pp$Symbol, "', '",
                     pp$Date, "', '",
                     pp$Strategy, "', ",
                     as.numeric( pp$Indicator ), ", '",
                     pp$Details,
                     "') on duplicate key update indicator=",
                     as.numeric( pp$Indicator ), ", details='", pp$Details, "'" )
         # print( qq )
         dbSendQuery( conn, statement=qq )
      }

      dbDisconnect( conn )
   }
}

writeActionsToDb = function( actions, symbol, strategy, date, dbName, user, password, group )
{
   require( RMySQL )

   conn = NULL

   if( missing( group ) )
   {
      conn = tryCatch(
                  dbConnect( MySQL(), dbname=dbName, user=user, password=password ),
                  error = function( ee ) NULL,
                  warning = function( ww ) NULL )
   }
   else
   {
      conn = tryCatch(
                  dbConnect( MySQL(), group=group ),
                  error = function( ee ) NULL,
                  warning = function( ww ) NULL )
   }

   if( !is.null( conn ) )
   {
      for( ii in 1:NROW(actions) )
      {
         aa = actions[ii,]
         qq = paste( sep="",
                     "insert into actions (symbol, date, strategy, position, price, pct) values ('",
                      symbol, "', '",
                      date, "', '",
                      strategy, "', ",
                      as.numeric( aa$Position ), ", ",
                      as.numeric( aa$Close ), ", ",
                      as.numeric( aa$Pct ), 
                      ") on duplicate key update ",
                      "position=", as.numeric( aa$Position ) )
         # print( qq )
         dbSendQuery( conn, statement=qq )
      }

      dbDisconnect( conn )
   }
}

appendNextBizday = function( x, holidays )
{
   require( timeDate, quietly=TRUE )

   dt = time( x )
   cd = coredata( x )

   lastDate = last( dt )
   nextDate = lastDate + 1

   if( missing( holidays ) )
   {
      # Get the holidays we will need. Use the year of the first date as start,
      # but use the year after the year of the last date as an end. This way
      # the function should work even for dates towards the year end.
      firstYear = as.numeric( format( first( dt ), "%Y" ) )
      lastYear = as.numeric( format( last( dt ), "%Y" ) ) + 1

      holidays = holidayNYSE( firstYear:lastYear )
   }

   repeat
   {
      if( isBizday( timeDate( nextDate ), holidays=holidays ) )
      {
         break
      }

      nextDate = nextDate + 1
   }

   mm = matrix( rep( 0, NCOL( cd ) ), nrow=1, ncol=NCOL( cd ) )

   res = rbind( x, xts( mm, order.by=nextDate ) )

   return( reclass( res, x ) )
}

computeOneDVIAction = function( close, x )
{
   x[tail( index( x ), 1 )] = close
   dvi = DVI( x, n=252, wts=c(0.8,0.2), smooth=3, magnitude=c(5,100,5), stretch=c(10,100,2) )
   val = as.numeric( tail( dvi$dvi, 1 ) )

   # Short if DVI > 0.5, long otherwise
   if( is.na( val ) )
   {
      return( 0 )
   }
   else if( val > 0.5 )
   {
      return( -1 )
   }

   return( 1 )
}

computeDVIActionPar = function( x, step=0.01, range=5, trace=F, cores )
{
   require( quantmod, quietly=TRUE )
   require( parallel, quietly=TRUE )

   prices = c( )
   positions = c( )

   latestClose = as.numeric( coredata( last( x ) ) )

   # Shift to the left to use the last entry as the "guessed" close
   yy = lag( x, -1 )

   # range is percentages
   range = range / 100

   # Compute the vector with all closing prices within the range
   close = latestClose * ( 1 - range )
   lastClose = latestClose * ( 1 + range )

   close = round( close / step ) * step
   numSteps = ( close - latestClose ) / step + 1

   close = round( close, 2 )
   lastClose = ceiling( lastClose * 100 ) / 100

   closes = close

   repeat
   {
      if( close >= lastClose ) break

      close = round( latestClose + step*numSteps, 2 )

      numSteps = numSteps + 1

      closes = c( closes, close )
   }

   # Detect the cores if not supplied
   if( missing( cores ) )
   {
      cores = parallel:::detectCores()
   }

   res = mclapply( closes, 
                   computeOneDVIAction,
                   x = yy,
                   mc.cores = cores )

   if( trace )
   {
      for( ii in 1:length( closes ) )
      {
         cat( paste( sep="",
                     round( ( closes[ii] - latestClose ) /
                            latestClose * 100, 2 ), "%, ",
                     round( closes[ii], 2 ), ", ",
                     res[[ii]], "\n" ) )
      }
   }

   # Summarize the positions
   prices = c()
   pcts = c()
   positions = c()

   # Impossible position
   lastPosition = -1e9

   len = length( closes )
   for( ii in 1:(len - 1) )
   {
      if( res[[ii]] != lastPosition )
      {
         positions = append( positions, res[[ii]] )
         prices = append( prices, closes[ii] )
         pcts = append( pcts, round( ( closes[ii] - latestClose ) /
                                     latestClose * 100, 2 ) )
         lastPosition = res[[ii]]
      }
   }

   positions = append( positions, res[[len]] )
   prices = append( prices, closes[ii] )
   pcts = append( pcts, round( ( closes[len] - latestClose ) /
                               latestClose * 100, 2 ) )

   df = data.frame( prices, pcts, positions )
   colnames( df ) = c( "Close", "Pct", "Position" )

   return( df )
}

computeOneMAAction = function( close, x, maType, ... )
{
   x[tail( index( x ), 1 )] = close
   ma = do.call( maType, list( x, ... ) )
   val = as.numeric( tail( ma, 1 ) )

   if( is.na( val ) )
   {
      return( 0 )
   }
   else if( val > close )
   {
      return( -1 )
   }

   return( 1 )
}

computeMAActionPar = function( x, maType=SMA, step=0.01, range=5, trace=F, cores, ... )
{
   require( quantmod, quietly=TRUE )
   require( parallel, quietly=TRUE )

   prices = c( )
   positions = c( )

   latestClose = as.numeric( coredata( last( x ) ) )

   # Shift to the left to use the last entry as the "guessed" close
   yy = lag( x, -1 )

   # range is percentages
   range = range / 100

   close = latestClose * ( 1 - range )
   lastClose = latestClose * ( 1 + range )

   close = round( close / step ) * step
   numSteps = ( close - latestClose ) / step + 1

   close = round( close, 2 )
   lastClose = ceiling( lastClose * 100 ) / 100

   closes = close

   repeat
   {
      if( close >= lastClose ) break

      close = round( latestClose + step*numSteps, 2 )

      numSteps = numSteps + 1

      closes = c( closes, close )
   }

   if( missing( cores ) )
   {
      cores = parallel:::detectCores()
   }

   res = mclapply( closes, 
                   computeOneMAAction,
                   x = yy,
                   maType=maType,
                   ..., 
                   mc.cores = cores )

   if( trace )
   {
      for( ii in 1:length( closes ) )
      {
         cat( paste( sep="",
                     round( ( closes[ii] - latestClose ) / latestClose * 100, 2 ), "%, ",
                     round( closes[ii], 2 ), ", ",
                     res[[ii]], "\n" ) )
      }
   }

   # Summarize the positions
   prices = c()
   pcts = c()
   positions = c()

   # Impossible position
   lastPosition = -1e9

   len = length( closes )
   for( ii in 1:(len - 1) )
   {
      if( res[[ii]] != lastPosition )
      {
         positions = append( positions, res[[ii]] )
         prices = append( prices, closes[ii] )
         pcts = append( pcts, round( ( closes[ii] - latestClose ) / latestClose * 100, 2 ) )
         lastPosition = res[[ii]]
      }
   }

   positions = append( positions, res[[len]] )
   prices = append( prices, closes[ii] )
   pcts = append( pcts, round( ( closes[len] - latestClose ) / latestClose * 100, 2 ) )

   df = data.frame( prices, pcts, positions )
   colnames( df ) = c( "Close", "Pct", "Position" )

   return( df )
}

rsiPredictions = function( ss, trimNAs=TRUE, ... )
{
   cl = Cl( ss )
   rsi = RSI( cl, ... )

   sigUp = lag( ifelse( rsi < 10, 1, 0 ), 1 )
   sigDown = lag( ifelse( rsi > 90, -1, 0 ), 1 )

   if( trimNAs )
   {
      sigUp = na.trim( sigUp )
      sigDown = na.trim( sigDown )
   }
   else
   {
      sigUp[is.na( sigUp )] = 0
      sigDown[is.na( sigDown )] = 0
   }

   sig = sigUp + sigDown

   res = merge( reclass( sig, ss ), sigUp, sigDown, lag( rsi[,1] ) )
   colnames( res ) = c( "Indicator", "Up", "Down", "RSI" )

   return( res )
}

dviPredictions = function( ss, trimNAs=TRUE, ... )
{
   cl = Cl( ss )
   dvi = DVI( cl, ... )

   sigUp = lag( ifelse( dvi$dvi < 0.5, 1, 0 ), 1 )
   sigDown = lag( ifelse( dvi$dvi > 0.5, -1, 0 ), 1 )

   if( trimNAs )
   {
      sigUp = na.trim( sigUp )
      sigDown = na.trim( sigDown )
   }
   else
   {
      sigUp[is.na( sigUp )] = 0
      sigDown[is.na( sigDown )] = 0
   }

   sig = sigUp + sigDown

   res = merge( reclass( sig, ss ), sigUp, sigDown, lag( dvi$dvi ) )
   colnames( res ) = c( "Indicator", "Up", "Down", "DVI" )

   return( res )
}

maPredictions = function( ss, maType=SMA, ... )
{
   cl = Cl( ss )
   ma = na.trim( maType( cl, ... ) )

   merged = merge( cl, ma, all=F )

   mm = merged[,1] - merged[,2]

   sigUp = na.trim( lag( ifelse( mm >= 0, 1, 0 ), 1 ) )
   sigDown = na.trim( lag( ifelse( mm < 0, -1, 0 ), 1 ) )

   sig = sigUp + sigDown

   res = merge( reclass( sig, ss ), sigUp, sigDown, lag( merged[,1] ), lag( merged[,2] ) )
   colnames( res ) = c( "Indicator", "Up", "Down", "Close", "MA" )

   return( res )
}

loadIndicator = function( fileName )
{
   require( quantmod, quietly=TRUE )
   return( as.xts( read.zoo(file=fileName, format="%Y-%m-%d", header=T, sep=",") ) )
}

writeIndicator = function( indicator, fileName )
{
   require( quantmod, quietly=TRUE )
   write.zoo( indicator, quote=F, row.names=F, sep=",", fileName )
}
