#' Computes Gains/Lift table
#' 
#' The gains column shows, for each group, the percent difference between the
#' overall proportion of events and the observed proportion of observations in
#' the group that are events. The cumulative gains column shows the percent
#' difference between the overall proportion of events and the observed
#' proportion of events in all groups up to the current group. The lift column
#' gives, for each group, the ratio of the proportion of observations in the
#' group that are events to the overall proportion of events. The cumulative
#' lift column gives event rate up to the current group over the overall event
#' rate. The percent captured column gives, for each group, the percentage of
#' total events in the group. The cumulative percent captured column gives the
#' percentage of total events up to the current group. The percent response
#' column gives the percentage of observations in the group that are events.
#' The cumulative percent response column gives the percentage of observations
#' up to the current group that are events. Note that percent response and
#' cumulative percent response are often considered gains and cumulative gains.
#' 
#' The statistics are computed as follows:
#'   
#' E  = total number of events
#' N  = number of observations
#' G  = number of groups (10 for deciles or 20 for demi-deciles)
#' P  = overall proportion of observations that are events (P = E/N)
#' ei = number of events in group i , i=1,2,...,G
#' ni = number of observations in group i
#' pi = proportion of observations in group i that are events (pi = ei/ni)
#' 
#' Response = pi = ei/ni
#' Cumulative Response = ??iei/??ini
#' Captured = ei/E
#' Cumulative Captured = ??iei/E
#' Lift = pi/P
#' Cumulative Lift = (??iei/??ini)/P
#' Gain = 100*(pi-P)/P = 100*(Lift-1)
#' Cumulative Gain = 100*(Cumulative Lift-1)
#' 
#' Assuming an unpredictive model which randomly puts observations into groups
#' using the overall observed proportion of events, baselines for each statistic
#' can be computed as follows:
#'   
#' %Response = 100*P
#' Cumulative %Response = 100*P
#' %Captured = 100*ni/N
#' Cumulative %Captured = 100*??ini/N
#' Lift = 1
#' Cumulative Lift = 1
#' Gain = 0
#' Cumulative Gain = 0
#' 
#' @param preds Prediction for elements, must be a score in the range [0, 1]
#' @param targets Actual class of elements. It can be a numeric binary vector
#'                (0|1), or a factor. If it is a factor, then the positive 
#'                parameter must be supplied. If positive is not provided, then
#'                take last class as the objective of the prediction
#' @param positive (optional) In case that targets is a factor, this param 
#'                 controls which class to use as the objective of the 
#'                 prediction. Default = NULL
#' @param num.groups (optional) Number of groups to use. Default = 10
#' 
#' @return A data frame with de following columns:
#' \describe{
#'   \item{group}{group number}
#'   \item{cumulative_data_fraction}{cumulative proportion of quantile size}
#'   \item{lower_threshold}{minimum prediction of quantile}
#'   \item{lift}{ratio of the proportion of observations in the group that are
#'         events to the overall proportion of events}
#'   \item{cumulative_lift}{event rate up to the current group over the overall
#'         event rate}
#'   \item{response_rate}{ratio of observations in the group that are events}
#'   \item{cumulative_response_rate}{ratio of observations up to the current
#'         group that are events}
#'   \item{capture_rate}{ratio of total events in the group}
#'   \item{cumulative_capture_rate}{ratio of total events up to the current
#'         group}
#'   \item{gain}{for each group, the percent difference between the overall
#'         proportion of events and the observed proportion of observations in
#'         the group that are events}
#'   \item{cumulative_gain}{percent difference between the overall proportion of
#'         events and the observed proportion of events in all groups up to the
#'         current group}
#' }
#' 
#' @references http://support.sas.com/kb/41/683.html
#' 
computeLift <- function(preds, targets, positive=NULL, num.groups = 10) {
  if (length(preds) != length(targets))
    return(NULL)
  #stop("Predictions and targets must be of the same length", call.=FALSE)
  
  # If targets is character, convert it to factor
  if (class(targets) == 'character') {
    targets <- factor(targets) #, levels(targets)[c(2,1)])
  }
  if (class(targets) == 'factor') {
    if (is.null(positive))
      positive <- levels(targets)[nlevels(targets)] # Use last level
    
    # If targets is a factor with more than two levels, reduce it to only two levels
    if (nlevels(targets) > 2) {
      #print(table(targets))
      l <- levels(targets)
      cc <- c()
      for(i in seq_along(l)) cc <- c(cc, ifelse(l[i] == positive, positive, paste0("NON ", positive)))
      levels(targets) <- cc
      #print(table(targets))
    }
    
    # Once that targets is a factor with two levels, make sure that the positive class is in proper position
    if (which(levels(targets) == positive) != 2) {
      targets <- factor(targets, levels(targets)[c(2,1)])
    }
    #print(table(targets))
    
    # Finally convert it to numeric [0, 1] 
    positive_str <- paste0('\'Positive\' Class : ', levels(targets)[2], '\n')
    targets <- as.numeric(targets) - 1 #
    #print(table(targets))
  }
  
  # Order predictions and their corresponding targets
  idxordpreds <- order(preds, decreasing = TRUE)
  preds <- preds[idxordpreds]
  targets <- targets[idxordpreds]
  
  N <- length(targets) # Number of observations
  
  # Get quantiles
  preds.quantiles <- split(preds, ceiling(seq_along(preds)/(N/num.groups)))
  targets.quantiles <- split(targets, ceiling(seq_along(targets)/(N/num.groups)))
  
  results.lift <- data.frame(group = 1:num.groups, cumulative_data_fraction = 0, lower_threshold = 0, 
                             lift = 0, cumulative_lift = 0,
                             response_rate = 0, cumulative_response_rate = 0,
                             capture_rate = 0, cumulative_capture_rate = 0,
                             gain = 0, cumulative_gain = 0,
                             e_i = 0, n_i = 0)
  
  E <- sum(unlist(lapply(targets.quantiles, sum))) # Total number of events
  P <- E/N # Overall response = Overall proportion of observations that are events
  
  for(i in 1:num.groups) {
    results.lift[i, "lower_threshold"] <- round(min(preds.quantiles[[i]]), digits = 6)
    
    target.quantile <- as.logical(targets.quantiles[[i]])
    
    # e_i
    e_i <- sum(target.quantile==TRUE)
    results.lift[i, "e_i"] <- e_i
    
    # n_i
    n_i <- length(target.quantile)
    results.lift[i, "n_i"] <- n_i
    
    # Response rate
    response.rate <- e_i/n_i # Response = p_i = e_i/n_i
    results.lift[i, "response_rate"] <- round(response.rate, digits = 6)
    
    # Lift
    lift <- response.rate/P # Lift = p_i/P
    results.lift[i, "lift"] <- round(lift, digits = 6)
    
    # Capture rate
    results.lift[i, "capture_rate"] <- round(e_i/E, digits = 6) # Capture = e_i/E
    
    # Gain
    results.lift[i, "gain"] <- round(100*(lift-1), digits = 6) # Gain = 100*(p_i - P)/P = 100*(Lift - 1)
    
    rm(target.quantile)
    
    # Cumulative Lift
    cum.lift <- sum(results.lift$e_i[1:i])/sum(results.lift$n_i[1:i])/P
    results.lift[i, "cumulative_lift"] <- round(cum.lift, digits = 6) # Cumulative Lift = Sum(e_i)/Sum(n_i)/P
    
    results.lift[i, "cumulative_data_fraction"] <- sum(results.lift$n_i[1:i])/N
    results.lift[i, "cumulative_response_rate"] <- round(sum(results.lift$e_i[1:i])/sum(results.lift$n_i[1:i]), digits = 6) # Cumulative Response = Sum(e_i)/Sum(n_i)
    results.lift[i, "cumulative_capture_rate"] <- round(sum(results.lift$capture_rate[1:i]), digits = 6) # Cumulative Capture = Sum(e_i)/E
    results.lift[i, "cumulative_gain"] <- round(100*(cum.lift - 1), digits = 6) # Cumulative Gain = 100*(Cumulative Lift - 1)
  }
  results.lift[num.groups, "cumulative_capture_rate"] <- 1.0
  
  results.lift$e_i <- NULL
  results.lift$n_i <- NULL
  
  attr(results.lift, "header") <- paste0(positive_str, "Gains/Lift Table:")
  attr(results.lift, "formats") <- c("%d", "%.8f", "%5f", "%5f", "%5f", "%5f", "%5f", "%5f", "%5f", "%5f", "%5f")
  attr(results.lift, "description") <- paste0("Avg response rate: ", 
                                              round(100*results.lift[num.groups, "cumulative_response_rate"], digits = 2), " %")
  #attr(results.lift, "P") <- P
  #attr(results.lift, "N") <- N
  
  class(results.lift) <- c("lift", "data.frame")
  return(results.lift)
}

print.lift <- function(obj) {
  cat(attr(obj, "header"), attr(obj, "description"), "\n")
  print.data.frame(obj)
}

plot.lift <- function(obj) {
  p <- par()
  mfrow <- p$mfrow
  par(mfrow=c(2,1))
  
  # Response plot
  # plot(obj$cumulative_data_fraction, obj$response, type = "b", 
  #      main = "Response", xlab="cumulative data fraction", ylab="response")
  # abline(h=attr(obj, "P")) # Baseline
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Capture plot
  # plot(obj$cumulative_data_fraction, obj$capture, type = "b", 
  #      main = "Capture", xlab="cumulative data fraction", ylab="capture")
  # abline(h=(1/nrow(obj))) # Baseline
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Lift plot
  plot(obj$cumulative_data_fraction, obj$lift, type = "b", 
       main = "Lift", xlab="cumulative data fraction", ylab="lift")
  abline(h=1) # Baseline
  abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Gain plot
  # plot(obj$cumulative_data_fraction, obj$gain, type = "b", 
  #      main = "Gain", xlab="cumulative data fraction", ylab="gain")
  # abline(h=0) # Baseline
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Cumulative Response plot
  # plot(obj$cumulative_data_fraction, obj$cumulative_response, type = "b", 
  #      main = "Cumulative Response", xlab="cumulative data fraction", ylab="cumulative response")
  # abline(h=attr(obj, "P")) # Baseline
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Cumulative Capture plot
  # plot(obj$cumulative_data_fraction, obj$cumulative_capture, type = "b", 
  #      main = "Cumulative Capture", xlab="cumulative data fraction", ylab="cumulative capture")
  # # Baseline
  # n_i <- obj$cumulative_data_fraction
  # #N <- attr(obj, "N")
  # lines(obj$cumulative_data_fraction, n_i, type = "l")
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Cumulative Lift plot
  plot(obj$cumulative_data_fraction, obj$cumulative_lift, type = "b", 
       main = "Cumulative Lift", xlab="cumulative data fraction", ylab="cumulative lift")
  abline(h=1) # Baseline
  abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  # Cumulative Gain plot
  # plot(obj$cumulative_data_fraction, obj$cumulative_gain, type = "b", 
  #      main = "Cumulative Gain", xlab="cumulative data fraction", ylab="cumulative gain")
  # abline(h=0) # Baseline
  # abline(v=obj$cumulative_data_fraction[1], col = "red") # First quantile
  
  par(mfrow=mfrow)
}
