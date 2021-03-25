# active_users.jl
using Dates
using CSV
using DataFrames

col_types = [String, String, Int64, String, Int64, String, String, String, Int64, 
             String, Int64, Float64, Int64, String, String, Int64, Int64, String, 
             String, String, Int64, BigInt, BigInt, Float64, Int64]

raw_df = CSV.read("data/old_slurm_jobs.csv", types = col_types)

const DT_FORMAT = "yyyy-mm-dd HH:MM:SS"
const OUTFILE = "data/vnc_partition_timeseries.csv"


keep_row = (raw_df[:, :nodes_alloc] .> 0) .& (raw_df[:, :start_time] .!= "1969-12-31 19:00:00")
keep_row[findall(ismissing.(keep_row))] .= false 
df = raw_df[Array{Bool,1}(keep_row), :]


jobs = DataFrame(df[:, [:id_user, :user]])
jobs[!, :start_time] = DateTime.(df[:, :start_time], DT_FORMAT)
jobs[!, :end_time]   = DateTime.(df[:, :end_time], DT_FORMAT)



const DATE_RANGE = minimum(jobs[:, :start_time]):Day(1):maximum(jobs[:, :end_time])
const ALL_DAYS   = Date.(collect(DATE_RANGE))

# This function takes two arguments, `start_col` and `end_col`, which are 
# both dataframe columns with timestamps denoting the start time and the end 
# time of a job. The function returns a vector with the same length as the input 
# columns; and each element of the returned vector is a StepRange type, denoting
# the range of the jobs' runtimes.
function compute_daterange(start_col, end_col) 
    n = length(start_col)
    res = Array{StepRange{DateTime, Day}}(undef, n)
    for i in 1:n 
        if start_col[i] > end_col[i] 
            # this path only gets hit if the end time was missing or broken
            continue
        else 
            res[i] = start_col[i]:Day(1):end_col[i]
        end 
    end 
    res 
end 



function get_active_users(job_daterange, job_user)
    n = length(job_daterange)
    days_dict = Dict{Date, Set{String}}()
    for i in 1:n 
        if job_daterange[i].step == Day(0)
            # this path only gets hit if the end time was missing or broken
            continue 
        else 
            job_days = Date.(collect(job_daterange[i]))
            for xday in job_days 
                if xday in keys(days_dict)
                    push!(days_dict[xday], job_user[i])
                else 
                    days_dict[xday] = Set([job_user[i]])
                end
            end 
        end 
    end 
    n_days = length(days_dict)

    days_df = DataFrame(day = Array{Date,1}(undef, n_days),
                        n_users = zeros(Int, n_days))
    i = 1  
    for (k, v) in days_dict 
        days_df[i, :day] = k 
        days_df[i, :n_users] = length(v) 
        i += 1
    end 
    active_users = sort(days_df, :day)
    active_users
end 
        





jobs[:, :time_range] = compute_daterange(jobs[:, :start_time], jobs[:, :end_time])

active_users_dict = get_active_users(jobs[:, :time_range], jobs[:, :user])



CSV.write("data/daily_active_users.csv", active_users_dict)
