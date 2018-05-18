Function Create-Batches
{
    param(
        [string]$InputPermissionsFile
    )
    #Variables
    $data = import-csv $InputPermissionsFile
    #update Mailbox to be whatever Identifier we are going to use across the board for matchup
    #group by targetrecipientprimarysmtp $hashdata = all users initially
    $hashData = $data | Group-Object -property Mailbox -AsHashTable -AsString
    #group by trusteerecipientprimarysmtp
    $hashDataByDelegate = $data | Group user -AsHashTable -AsString
    #create array for users with no permissions associations
    $usersWithNoDependents = New-Object System.Collections.ArrayList
    $batch = @{} #batches hashtable
    $batchNum = 0
    $hashDataSize = $hashData.Count #total count of permissioned users
    $yyyyMMdd = Get-Date -Format 'yyyyMMdd'

    try
    {
        #build an array of all users with no permission associations
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Build ArrayList for users with no dependents"
        If ($hashDataByDelegate["None"].count -gt 0)
        {
            $hashDataByDelegate["None"] | % {$_.Mailbox} | % {[void]$usersWithNoDependents.Add($_)}
        }

        #remove these users from the complete users hashtable
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Identify users with no permissions on them, nor them have perms on another"
        If ($usersWithNoDependents.count -gt 0)
        {
            $($usersWithNoDependents) | % {
                if ($hashDataByDelegate.ContainsKey($_))
                {
                    $usersWithNoDependents.Remove($_)
                }
            }

            Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Remove users with no dependents from hash"
            $usersWithNoDependents | % {$hashData.Remove($_)}
            #Clean out hashData of users in hash data with no delegates, otherwise they'll get batched
            Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Clean out hashData of users in hash with no delegates"
            foreach ($key in $($hashData.keys))
            {
                if (($hashData[$key] | select -expandproperty user ) -eq "None")
                {
                    $hashData.Remove($key)
                }
            }
        }
        #Execute batch functions
        If (($hashData.count -ne 0) -or ($usersWithNoDependents.count -ne 0))
        {
            Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Run function: Find-Links" -ForegroundColor White
            while ($hashData.count -ne 0) {Find-Links $hashData | out-null}
            Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Run function: Create-BatchFile" -ForegroundColor White
            Create-BatchFile $batch $usersWithNoDependents
        }
    }
    catch
    {
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Error: $_"
    }
}

Function Find-Links($hashData)
{
    try
    {
        $nextInHash = $hashData.Keys | select -first 1
        $batch.Add($nextInHash, $hashData[$nextInHash])

        Do
        {
            $checkForMatches = $false
            foreach ($key in $($hashData.keys))
            {
                Write-Progress -Activity "Step 2 of 3: Analyze Delegates" -status "Items remaining: $($hashData.Count)" `
                    -percentComplete (($hashDataSize - $hashData.Count) / $hashDataSize * 100)

                #Checks
                $usersHashData = $($hashData[$key]) | % {$_.mailbox}
                $usersBatch = $($batch[$nextInHash]) | % {$_.mailbox}
                $delegatesHashData = $($hashData[$key]) | % {$_.user}
                $delegatesBatch = $($batch[$nextInHash]) | % {$_.user}

                $ifMatchesHashUserToBatchUser = [bool]($usersHashData | ? {$usersBatch -contains $_})
                $ifMatchesHashDelegToBatchDeleg = [bool]($delegatesHashData | ? {$delegatesBatch -contains $_})
                $ifMatchesHashUserToBatchDelegate = [bool]($usersHashData | ? {$delegatesBatch -contains $_})
                $ifMatchesHashDelegToBatchUser = [bool]($delegatesHashData | ? {$usersBatch -contains $_})

                If ($ifMatchesHashDelegToBatchDeleg -OR $ifMatchesHashDelegToBatchUser -OR $ifMatchesHashUserToBatchUser -OR $ifMatchesHashUserToBatchDelegate)
                {
                    if (($key -ne $nextInHash))
                    {
                        $batch[$nextInHash] += $hashData[$key]
                        $checkForMatches = $true
                    }
                    $hashData.Remove($key)
                }
            }
        } Until ($checkForMatches -eq $false)

        return $hashData
    }
    catch
    {
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Error: $_" -ForegroundColor Red
    }
}

Function Create-BatchFile($batchResults, $usersWithNoDepsResults)
{
    try
    {
        "Batch,User" > $Script:BatchesFile
        foreach ($key in $batchResults.keys)
        {
            $batchNum++
            $batchName = "BATCH-$batchNum"
            $output = New-Object System.Collections.ArrayList
            $($batch[$key]) | % {$output.add($_.mailbox) | out-null}
            $($batch[$key]) | % {$output.add($_.user) | out-null}
            $output | select -Unique | % {
                "$batchName" + "," + $_ >> $Script:BatchesFile
            }
        }
        If ($usersWithNoDepsResults.count -gt 0)
        {
            $batchNum++
            foreach ($user in $usersWithNoDepsResults)
            {
                #$batchName = "BATCH-$batchNum"
                $batchName = "BATCH-NoDependencies"
                "$batchName" + "," + $user >> $Script:BatchesFile
            }
        }
    }
    catch
    {
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Error: $_" -ForegroundColor Red
    }
}

Function Create-MigrationSchedule
{
    param(
        [string]$InputBatchesFile
    )
    try
    {
        If (-not (Test-Path $InputBatchesFile))
        {
            throw [System.IO.FileNotFoundException] "$($InputBatchesFile) file not found."
        }
        $usersFromBatch = import-csv $InputBatchesFile
        "Migration Date(MM/dd/yyyy),Migration Window,Migration Group,PrimarySMTPAddress,SuggestedBatch,MailboxSize(MB),Notes" > $Script:MigrationScheduleFile
        $userInfo = New-Object System.Text.StringBuilder
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Number of users in the migration schedule: $($usersFromBatch.Count)" -ForegroundColor White

        $usersFromBatchCounter = 0
        foreach ($item in $usersFromBatch)
        {
            $usersFromBatchCounter++
            $usersFromBatchRemaining = $usersFromBatch.count - $usersFromBatchCounter
            Write-Progress -Activity "Step 3 of 3: Creating migration schedule" -status "Items remaining: $($usersFromBatchRemaining)" `
                -percentComplete (($usersFromBatchCounter / $usersFromBatch.count) * 100)

            #Check if using UseImportCSVFile and if yes, check if the user was part of that file, otherwise mark
            $isUserPartOfInitialCSVFile = ""
            If ($Script:InputMailboxesCSV -ne "")
            {
                If (-not ($Script:ListOfMailboxes.PrimarySMTPAddress -contains $item.user))
                {
                    $isUserPartOfInitialCSVFile = "User was not part of initial csv file"
                }
            }

            $user = get-user $item.user -erroraction SilentlyContinue

            If (![string]::IsNullOrEmpty($user.WindowsEmailAddress))
            {
                $mbStats = Get-MailboxStatistics $user.WindowsEmailAddress.tostring() | select totalitemsize
                If ($mbStats.totalitemsize.value)
                {
                    #if connecting through remote pshell, and not using Exo server shell, the data comes as
                    #TypeName: Deserialized.Microsoft.Exchange.Data.ByteQuantifiedSize
                    if ( ($mbStats.TotalItemSize.Value.GetType()).name.ToString() -eq "ByteQuantifiedSize")
                    {
                        $mailboxSize = $mbStats.totalitemsize.value.ToMb()
                    }
                    else
                    {
                        $mailboxSize = $mbStats.TotalItemSize.Value.ToString().split("(")[1].split(" ")[0].replace(",", "") / 1024 / 1024
                    }

                }
                Else
                {
                    $mailboxSize = 0
                }

                $userInfo.AppendLine(",,,$($user.WindowsEmailAddress),$($item.Batch),$($mailboxSize),$isUserPartOfInitialCSVFile") | Out-Null
            }
            Else
            {
                #there was an error either getting the user from Get-User or the user doesn't have an email address
                $userInfo.AppendLine(",,,$($item.user),$($item.Batch),n/a,,User not found or doesn't have an email address") | Out-Null
            }
        }
        $userInfo.ToString().TrimEnd() >> $Script:MigrationScheduleFile
    }
    catch
    {
        Write-LogEntry -LogName:$Script:LogFile -LogEntryText "Error: $($_) at $($_.InvocationInfo.ScriptLineNumber)" -ForegroundColor Red
    }
}