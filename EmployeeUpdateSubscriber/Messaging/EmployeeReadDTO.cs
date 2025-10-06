namespace EmployeeUpdateSubscriber.Messaging
{
    public class EmployeeReadDTO
    {
        public required string Id { get; set; }
        public required string Name { get; set; }
        public required string Address { get; set; }
        public required string Email { get; set; }
        public required string Phone { get; set; }
    }
}
