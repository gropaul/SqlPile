from extract_strings import extract_strings

src = """
private const string CoreQuery = @"SELECT COUNT(*) OVER () AS TotalResultCount,
		                    v.VacancyId,
		                    v.VacancyOwnerRelationshipId,
		                    v.VacancyReferenceNumber,
		                    v.VacancyGuid,
		                    v.VacancyStatusId,
		                    v.VacancyTypeId,
		                    v.Title,
		                    v.AddressLine1,
		                    v.AddressLine2,
		                    v.AddressLine3,
		                    v.AddressLine4,
		                    v.AddressLine5,
		                    v.Town,
		                    v.CountyId,
		                    v.PostCode,
		                    v.ApplyOutsideNAVMS,
		                    v.ApplicationClosingDate,
		                    v.NoOfOfflineApplicants,
		                    CASE v.ApplyOutsideNAVMS
 			                    WHEN 1 THEN 0
			                    ELSE dbo.GetApplicantCount(v.VacancyId) 
		                    END
		                    AS ApplicantCount,
		                    CASE v.ApplyOutsideNAVMS
 			                    WHEN 1 THEN 0
			                    ELSE dbo.GetNewApplicantCount(v.VacancyId)
		                    END
		                    AS NewApplicantCount,
		                    dbo.GetFirstSubmittedDate(v.VacancyID) AS DateFirstSubmitted,
		                    dbo.GetSubmittedDate(v.VacancyID) AS DateSubmitted,
		                    dbo.GetCreatedDate(v.VacancyID) AS CreatedDate,
		                    e.FullName AS EmployerName,
		                    dbo.GetDateQAApproved(v.VacancyID) AS DateQAApproved,
		                    e.EmployerId,
		                    e.Town AS EmployerLocation,
		                    p.TradingName as ProviderTradingName,
		                    v.SubmissionCount,
		                    CASE
			                    WHEN (v.StandardId IS NULL) THEN af.CodeName
			                    ELSE NULL 
		                    END 
		                    AS FrameworkCodeName,
		                    el.CodeName AS ApprenticeshipLevel,
		                    ao.CodeName AS SectorCodeName,
		                    dbo.GetCreatedByProviderUsername(v.VacancyId) AS CreatedByProviderUsername,
		                    dbo.GetDateQAApproved(v.VacancyId) AS DateQAApproved,
		                    rt.TeamName AS RegionalTeam,
		                    COALESCE(v.StandardId, af.StandardId) AS StandardId,
                            v.StartedToQADateTime,
                            v.QAUserName,
                            v.ContractOwnerId,
                            v.ExpectedStartDate AS PossibleStartDate,
                            v.NumberOfPositions,
                            v.WageType,
                            v.WageUnitId,
                            v.WeeklyWage,
                            v.WageLowerBound,
                            v.WageUpperBound,
                            v.WageText,
                            v.HoursPerWeek,
                            v.ShortDescription,
                            v.DeliveryOrganisationId,
                            v.DurationValue AS Duration,
		                    v.ExpectedDuration,
		                    v.VacancyLocationTypeId,
		                    v.VacancyManagerId,
                            v.TrainingTypeId,
                            v.VacancyLocationTypeId,
                            v.VacancyManagerId,
                            v.WorkingWeek,  
                            v.MasterVacancyId,
                            v.Latitude,
                            v.Longitude,
                            v.GeocodeEasting,
                            v.GeocodeNorthing,
                            v.EmployerAnonymousName,
                            v.UpdatedDateTime
                    FROM	Vacancy v
                    JOIN	VacancyOwnerRelationship o
                    ON		o.VacancyOwnerRelationshipId = v.VacancyOwnerRelationshipId
                    JOIN	Employer e
                    ON		o.EmployerId = e.EmployerId
                    JOIN	Provider p
                    ON		p.ProviderID = v.ContractOwnerID
                    JOIN	ProviderSite s
                    ON      s.ProviderSiteId = v.VacancyManagerId
                    LEFT OUTER JOIN	ApprenticeshipFramework af
                    ON		af.ApprenticeshipFrameworkId = v.ApprenticeshipFrameworkId
                    LEFT OUTER JOIN	ApprenticeshipType AS at
                    ON		at.ApprenticeshipTypeId = v.ApprenticeshipType
                    LEFT OUTER JOIN	Reference.EducationLevel el
                    ON		el.EducationLevelId = at.EducationLevelId
                    LEFT OUTER JOIN	ApprenticeshipOccupation ao
                    ON		v.SectorId = ao.ApprenticeshipOccupationId
                    LEFT OUTER JOIN	RegionalTeamMappings t
                    ON		s.PostCode LIKE t.PostcodeStart + '[0-9]%'
                    LEFT OUTER JOIN	RegionalTeams rt
                    ON		rt.Id = t.RegionalTeam_Id";
"""

import pytest

if __name__ == "__main__":
    strings = extract_strings(src, is_src=True, language="csharp", dedupe=True)
    print(strings)